use crate::OptimizerContext;

/// Errors produced by optimization passes.
#[derive(Debug)]
pub enum OptimizeError {
    /// A pass returned an error with a message.
    PassError { pass: &'static str, message: String },
    /// The pass manager reached its iteration limit without converging.
    MaxIterationsReached,
}

impl std::fmt::Display for OptimizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PassError { pass, message } => write!(f, "pass '{pass}' failed: {message}"),
            Self::MaxIterationsReached => write!(f, "optimizer reached max iterations"),
        }
    }
}

impl std::error::Error for OptimizeError {}

/// Result type for optimization operations.
pub type OptimizeResult<T> = Result<T, OptimizeError>;

/// Base trait for all optimizer passes.
pub trait Pass {
    fn name(&self) -> &'static str;
}

/// Whether a pass changed the reachable plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassResult {
    Unchanged,
    Changed,
}

/// A pass that rewrites a full query.
pub trait QueryPass: Pass {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult>;
}

/// Runs query passes in registration order until no pass reports changes.
pub struct PassManager {
    passes: Vec<Box<dyn QueryPass>>,
    max_iterations: usize,
}

impl PassManager {
    /// Creates a pass manager with the given iteration limit.
    pub fn new(max_iterations: usize) -> Self {
        Self {
            passes: Vec::new(),
            max_iterations,
        }
    }

    /// Registers a pass to run in order.
    pub fn add_pass(&mut self, pass: impl QueryPass + 'static) {
        self.passes.push(Box::new(pass));
    }

    /// Runs all passes in a fixpoint loop until convergence or `max_iterations`.
    pub fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<()> {
        for _ in 0..self.max_iterations {
            let mut any_changed = false;
            for pass in &mut self.passes {
                if pass.run(ctx)? == PassResult::Changed {
                    any_changed = true;
                }
            }
            if !any_changed {
                return Ok(());
            }
        }
        Err(OptimizeError::MaxIterationsReached)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryContext;

    struct NoopPass;
    impl Pass for NoopPass {
        fn name(&self) -> &'static str {
            "noop"
        }
    }
    impl QueryPass for NoopPass {
        fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
            Ok(PassResult::Unchanged)
        }
    }

    struct AlwaysChangesPass(usize);
    impl Pass for AlwaysChangesPass {
        fn name(&self) -> &'static str {
            "always_changes"
        }
    }
    impl QueryPass for AlwaysChangesPass {
        fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
            self.0 += 1;
            Ok(PassResult::Changed)
        }
    }

    #[test]
    fn noop_pass_converges_in_one_iteration() {
        let mut pm = PassManager::new(10);
        pm.add_pass(NoopPass);
        let mut ctx = OptimizerContext::new(QueryContext::new());
        pm.run(&mut ctx).unwrap();
    }

    #[test]
    fn always_changes_pass_hits_max_iterations() {
        let mut pm = PassManager::new(3);
        pm.add_pass(AlwaysChangesPass(0));
        let mut ctx = OptimizerContext::new(QueryContext::new());
        assert!(matches!(
            pm.run(&mut ctx),
            Err(OptimizeError::MaxIterationsReached)
        ));
    }

    #[test]
    fn pass_manager_runs_passes_in_order() {
        use std::sync::{Arc, Mutex};

        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        struct LogPass {
            name: &'static str,
            log: Arc<Mutex<Vec<&'static str>>>,
            calls: usize,
        }
        impl Pass for LogPass {
            fn name(&self) -> &'static str {
                self.name
            }
        }
        impl QueryPass for LogPass {
            fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                self.log.lock().unwrap().push(self.name);
                self.calls += 1;
                // Report Changed on first call to trigger a second iteration.
                if self.calls == 1 {
                    Ok(PassResult::Changed)
                } else {
                    Ok(PassResult::Unchanged)
                }
            }
        }

        let mut pm = PassManager::new(10);
        pm.add_pass(LogPass {
            name: "a",
            log: Arc::clone(&log),
            calls: 0,
        });
        pm.add_pass(LogPass {
            name: "b",
            log: Arc::clone(&log),
            calls: 0,
        });

        let mut ctx = OptimizerContext::new(QueryContext::new());
        pm.run(&mut ctx).unwrap();

        assert_eq!(*log.lock().unwrap(), vec!["a", "b", "a", "b"]);
    }
}
