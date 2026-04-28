use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tracing::info;

use crate::{
    error::Result,
    ir::{IRContext, Operator},
};

use super::PassExtension;

/// Pass extension that emits wall-clock timing metrics for each pass run.
///
/// Metrics are logged through `tracing`, so callers can enable or suppress
/// output with their subscriber configuration. Without an explicit target, the
/// events use this module path as their target.
#[derive(Default)]
pub struct PassProfilingExtension {
    /// Mutable timing state shared across pass invocations.
    state: Mutex<ProfilingPassState>,
}

/// Internal state accumulated by [`PassProfilingExtension`].
#[derive(Default)]
struct ProfilingPassState {
    /// Passes that have started but not yet finished.
    active_passes: Vec<ActivePass>,
    /// Aggregated per-pass timing totals keyed by pass name.
    totals: HashMap<&'static str, PassProfileMetric>,
}

/// Timing information for a pass that is currently executing.
struct ActivePass {
    /// Stable name of the in-flight pass.
    name: &'static str,
    /// Timestamp captured immediately before the pass started.
    started_at: Instant,
}

/// Aggregated metrics for one pass name.
#[derive(Default)]
struct PassProfileMetric {
    /// Number of times the pass has been executed.
    invocations: usize,
    /// Total wall-clock time spent running the pass.
    total_duration: Duration,
}

/// Formats a duration in milliseconds using 3 significant digits.
fn format_duration_ms(duration: Duration) -> String {
    let value = duration.as_secs_f64() * 1_000.0;
    if value == 0.0 {
        return "0".to_string();
    }

    let exponent = value.abs().log10().floor() as i32;
    let scale = 10f64.powi(2 - exponent);
    let rounded = (value * scale).round() / scale;
    let decimals = (2 - exponent).max(0) as usize;
    format!("{rounded:.decimals$}")
}

impl PassExtension for PassProfilingExtension {
    fn before_pass(
        &self,
        pass_name: &'static str,
        _root: &Arc<Operator>,
        _ctx: &IRContext,
    ) -> Result<()> {
        self.state.lock().unwrap().active_passes.push(ActivePass {
            name: pass_name,
            started_at: Instant::now(),
        });
        Ok(())
    }

    fn after_pass(
        &self,
        pass_name: &'static str,
        before: &Arc<Operator>,
        after: &Arc<Operator>,
        _ctx: &IRContext,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let active = state
            .active_passes
            .pop()
            .expect("after_pass called without a matching before_pass");
        assert_eq!(
            active.name, pass_name,
            "after_pass order mismatch: expected {pass_name}, found {}",
            active.name
        );
        let elapsed = active.started_at.elapsed();
        let metric = state.totals.entry(pass_name).or_default();
        metric.invocations += 1;
        metric.total_duration += elapsed;
        info!(
            pass = pass_name,
            changed = before != after,
            elapsed_ms = %format_duration_ms(elapsed),
            total_ms = %format_duration_ms(metric.total_duration),
            invocations = metric.invocations,
            "optimizer pass profile",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn format_duration_ms_uses_three_significant_digits() {
        assert_eq!(super::format_duration_ms(Duration::from_nanos(0)), "0");
        assert_eq!(
            super::format_duration_ms(Duration::from_micros(456)),
            "0.456"
        );
        assert_eq!(
            super::format_duration_ms(Duration::from_micros(12_340)),
            "12.3"
        );
        assert_eq!(
            super::format_duration_ms(Duration::from_micros(123_400)),
            "123"
        );
        assert_eq!(
            super::format_duration_ms(Duration::from_micros(1_234_000)),
            "1230"
        );
    }
}
