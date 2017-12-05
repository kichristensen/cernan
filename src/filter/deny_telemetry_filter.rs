//! Filter any telemetry from the incoming stream 
//!
//! This filter is intended to remove LogLines from the incoming stream. This is
//! useful for situations where a mixed stream is being fed into a sink that
//! cannot use them. While such sinks should filter LogLines it can be useful to
//! employ this filter to reduce the load on an incoming hopper queue.

use filter;
use metric;

/// Filter LogLines from the incoming stream
pub struct DenyTelemetryFilter {
}

/// Configuration for `DelayFilter`
#[derive(Clone, Debug)]
pub struct DenyTelemetryFilterConfig {
    /// The filter's unique name in the routing topology.
    pub config_path: Option<String>,
    /// The forwards along which the filter will emit its `metric::Event`s.
    pub forwards: Vec<String>,
}

impl DenyTelemetryFilter {
    /// Create a new DelayFilter
    pub fn new(_config: &DenyTelemetryFilterConfig) -> DenyTelemetryFilter {
        DenyTelemetryFilter {
        }
    }
}

impl filter::Filter for DenyTelemetryFilter {
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Telemetry(m) => {
                res.push(metric::Event::Telemetry(m));
            },
            metric::Event::Log(_) => {
                // do nothing, intentionally 
            },
            metric::Event::TimerFlush(f) => {
                res.push(metric::Event::TimerFlush(f));
            }
        }
        Ok(())
    }
}
