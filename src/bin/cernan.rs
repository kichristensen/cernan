extern crate chrono;
extern crate cernan;
extern crate hopper;
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;

use cernan::filter::{Filter, ProgrammableFilterConfig};
use cernan::metric;

use cernan::sink::FirehoseConfig;
use cernan::sink::Sink;
use cernan::source::Source;
use cernan::util;
use slog::Drain;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::process;
use std::str;
use std::sync;
use std::thread;

fn populate_forwards(
    sends: &mut util::Channel,
    mut top_level_forwards: Option<&mut HashSet<String>>,
    forwards: &[String],
    config_path: &str,
    log: sync::Arc<sync::Mutex<slog::Logger>>,
    available_sends: &HashMap<String, hopper::Sender<metric::Event>>,
) {
    for fwd in forwards {
        if let Some(tlf) = top_level_forwards.as_mut() {
            let _ = (*tlf).insert(fwd.clone());
        }
        match available_sends.get(fwd) {
            Some(snd) => {
                sends.push(snd.clone());
            }
            None => {
                error!(log.lock().unwrap(),
                       "Unable to fulfill configured forward";
                       "config_path" => config_path,
                       "forward" => fwd
                );
                process::exit(0);
            }
        }
    }
}

macro_rules! cfg_conf {
    ($config:ident) => {
        $config.config_path.clone().expect("[INTERNAL ERROR] no config_path")
    }
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let root_log =
        slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
    let root_log = sync::Arc::new(sync::Mutex::new(root_log));

    let mut args = cernan::config::parse_args();

    // TODO set log level
    let level = match args.verbose {
        0 => slog::Level::Error,
        1 => slog::Level::Warning,
        2 => slog::Level::Info,
        3 => slog::Level::Debug,
        _ => slog::Level::Trace,
    };

    // fern::Dispatch::new()
    //     .format(|out, message, record| {
    //                 out.finish(format_args!("[{}][{}][{}][{}] {}",
    //                                         record.location().module_path(),
    //                                         record.location().line(),
    //                                         Utc::now().to_rfc3339(),
    //                                         record.level(),
    //                                         message))
    //             })
    //     .level(level)
    //     .chain(std::io::stdout())
    //     .apply()
    //     .expect("could not set up logging");

    info!(&root_log.lock().unwrap(), "cernan - {}", args.version);
    let mut joins = Vec::new();
    let mut sends: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    let mut flush_sends = HashSet::new();

    // SINKS
    //
    if let Some(config) = mem::replace(&mut args.null, None) {
        let (null_send, null_recv) =
            hopper::channel(&config.config_path, &args.data_directory).unwrap();
        sends.insert(config.config_path.clone(), null_send);
        joins.push(thread::spawn(
            move || { cernan::sink::Null::new(config).run(null_recv); },
        ));
    }
    if let Some(config) = mem::replace(&mut args.console, None) {
        let config_path = cfg_conf!(config);
        let (console_send, console_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), console_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Console::new(config).run(console_recv);
        }));
    }
    if let Some(config) = mem::replace(&mut args.wavefront, None) {
        let config_path = cfg_conf!(config);
        let (wf_send, wf_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), wf_send);
        let _log = root_log.lock().unwrap().new(o!(
                "sink" => "wavefront",
                ));
        joins.push(thread::spawn(move || {
            match cernan::sink::Wavefront::new(config, _log.clone()) {
                Ok(mut w) => {
                    w.run(wf_recv);
                }
                Err(e) => {
                    error!(_log, "Configuration error for Wavefront: {}", e);
                    process::exit(1);
                }
            }
        }));
    }
    if let Some(config) = mem::replace(&mut args.prometheus, None) {
        let config_path = cfg_conf!(config);
        let (prometheus_send, prometheus_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), prometheus_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Prometheus::new(config).run(prometheus_recv);
        }));
    }
    if let Some(config) = mem::replace(&mut args.influxdb, None) {
        let config_path = cfg_conf!(config);
        let (flx_send, flx_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), flx_send);
        let _log = root_log.lock().unwrap().new(o!(
                "sink" => "influxdb",
                ));
        joins.push(thread::spawn(move || {
            cernan::sink::InfluxDB::new(config, _log).run(flx_recv);
        }));
    }
    if let Some(config) = mem::replace(&mut args.native_sink_config, None) {
        let config_path = cfg_conf!(config);
        let (cernan_send, cernan_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), cernan_send);
        let _log = root_log.lock().unwrap().new(o!(
                "sink" => "native",
                ));
        joins.push(thread::spawn(move || {
            cernan::sink::Native::new(config, _log).run(cernan_recv);
        }));
    }

    if let Some(config) = mem::replace(&mut args.elasticsearch, None) {
        let config_path = cfg_conf!(config);
        let (cernan_send, cernan_recv) =
            hopper::channel(&config_path, &args.data_directory).unwrap();
        sends.insert(config_path.clone(), cernan_send);
        let _log = root_log.lock().unwrap().new(o!(
                "sink" => "elasticsearch",
                ));
        joins.push(thread::spawn(move || {
            cernan::sink::Elasticsearch::new(config, _log).run(cernan_recv);
        }));
    }

    if let Some(cfgs) = mem::replace(&mut args.firehosen, None) {
        for config in cfgs {
            let config_path = cfg_conf!(config);
            let f: FirehoseConfig = config.clone();
            let (firehose_send, firehose_recv) =
                hopper::channel(&config_path, &args.data_directory).unwrap();
            sends.insert(config_path.clone(), firehose_send);
            let _log = root_log.lock().unwrap().new(o!("sink" => "firehose"));
            joins.push(thread::spawn(move || {
                cernan::sink::Firehose::new(f, _log).run(firehose_recv);
            }));
        }
    }

    // // FILTERS
    // //
    if let Some(cfg_map) = mem::replace(&mut args.filters, None) {
        for config in cfg_map.values() {
            let c: ProgrammableFilterConfig = (*config).clone();
            let config_path = cfg_conf!(config);
            let (flt_send, flt_recv) =
                hopper::channel(&config_path, &args.data_directory).unwrap();
            sends.insert(config_path.clone(), flt_send);
            let mut downstream_sends = Vec::new();
            populate_forwards(
                &mut downstream_sends,
                None,
                &config.forwards,
                &config.config_path.clone().expect("[INTERNAL ERROR] no config_path"),
                root_log.clone(),
                &sends,
            );
            let _log = root_log
                .lock()
                .unwrap()
                .new(o!("filter" => config.config_path.clone()));
            joins.push(thread::spawn(move || {
                cernan::filter::ProgrammableFilter::new(c, _log)
                    .run(flt_recv, downstream_sends);
            }));
        }
    }

    // SOURCES
    //
    if let Some(cfg_map) = mem::replace(&mut args.native_server_config, None) {
        for (_, config) in cfg_map {
            let mut native_server_send = Vec::new();
            populate_forwards(
                &mut native_server_send,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                root_log.clone(),
                &sends,
            );
            let _log = root_log.lock().unwrap().new(o!("source" => "native"));
            joins.push(thread::spawn(move || {
                cernan::source::NativeServer::new(native_server_send, config, _log)
                    .run();
            }))
        }
    }

    let internal_config = args.internal;
    let mut internal_send = Vec::new();
    populate_forwards(
        &mut internal_send,
        Some(&mut flush_sends),
        &internal_config.forwards,
        &cfg_conf!(internal_config),
        root_log.clone(),
        &sends,
    );
    joins.push(thread::spawn(move || {
        cernan::source::Internal::new(internal_send, internal_config).run();
    }));

    if let Some(cfg_map) = mem::replace(&mut args.statsds, None) {
        for (_, config) in cfg_map {
            let mut statsd_sends = Vec::new();
            populate_forwards(
                &mut statsd_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                root_log.clone(),
                &sends,
            );
            let _log = root_log.lock().unwrap().new(o!("source" => "statsd"));
            joins.push(thread::spawn(move || {
                cernan::source::Statsd::new(statsd_sends, config, _log).run();
            }));
        }
    }

    if let Some(cfg_map) = mem::replace(&mut args.graphites, None) {
        for (_, config) in cfg_map {
            let mut graphite_sends = Vec::new();
            populate_forwards(
                &mut graphite_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                root_log.clone(),
                &sends,
            );
            let _log = root_log.lock().unwrap().new(o!("source" => "graphite"));
            joins.push(thread::spawn(move || {
                cernan::source::Graphite::new(graphite_sends, config, _log).run();
            }));
        }
    }

    if let Some(cfg) = mem::replace(&mut args.files, None) {
        for config in cfg {
            let mut fp_sends = Vec::new();
            populate_forwards(
                &mut fp_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                root_log.clone(),
                &sends,
            );
            let _log = root_log.lock().unwrap().new(o!("source" => "file_server"));
            joins.push(thread::spawn(move || {
                cernan::source::FileServer::new(fp_sends, config, _log).run();
            }));
        }
    }

    // BACKGROUND
    //
    joins.push(thread::spawn(move || {
        let mut flush_channels = Vec::new();
        for destination in &flush_sends {
            match sends.get(destination) {
                Some(snd) => {
                    flush_channels.push(snd.clone());
                }
                None => {
                    error!(
                        root_log.lock().unwrap(),
                        "Unable to fulfill configured top-level flush to {}",
                        destination
                    );
                    process::exit(0);
                }
            }
        }
        cernan::source::FlushTimer::new(flush_channels).run();
    }));

    joins.push(thread::spawn(move || { cernan::time::update_time(); }));

    for jh in joins {
        // TODO Having sub-threads panic will not cause a bubble-up if that
        // thread is not the currently examined one. We're going to have to have
        // some manner of sub-thread communication going on.
        jh.join().expect("Uh oh, child thread panicked!");
    }
}
