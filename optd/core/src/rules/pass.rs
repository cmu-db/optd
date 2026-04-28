use std::{future::Future, sync::Arc};

use crate::{
    error::Result,
    ir::{IRContext, Operator},
};

/// A named optimizer pass that rewrites an operator tree.
pub trait PlanPass {
    /// The stable pass name exposed to extensions.
    fn name(&self) -> &'static str;

    /// Runs the pass and returns the rewritten root operator.
    fn run(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>>;
}

/// Extension hook that can observe pass execution.
///
/// Implementors can use these callbacks for logging, metrics, assertions, or
/// pass-local instrumentation around a pass invocation.
pub trait PassExtension: Send + Sync {
    /// Called immediately before `pass` runs.
    fn before_pass(
        &self,
        _pass_name: &'static str,
        _root: &Arc<Operator>,
        _ctx: &IRContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Called after `pass` finishes successfully.
    fn after_pass(
        &self,
        _pass_name: &'static str,
        _before: &Arc<Operator>,
        _after: &Arc<Operator>,
        _ctx: &IRContext,
    ) -> Result<()> {
        Ok(())
    }
}

/// Registry and runner for pass extensions.
#[derive(Clone)]
pub struct PassManager {
    /// Registered extensions invoked around every pass execution.
    extensions: Arc<[Arc<dyn PassExtension>]>,
}

impl PassManager {
    /// Creates a builder used to register pass extensions.
    pub fn builder() -> PassManagerBuilder {
        PassManagerBuilder::default()
    }

    /// Runs `pass` and invokes all registered `before_pass` / `after_pass`
    /// extensions around it.
    pub fn run<P: PlanPass>(
        &self,
        pass: &P,
        root: Arc<Operator>,
        ctx: &IRContext,
    ) -> Result<Arc<Operator>> {
        for extension in self.extensions.iter() {
            extension.before_pass(pass.name(), &root, ctx)?;
        }

        let before = root.clone();
        let after = pass.run(root, ctx)?;

        for extension in self.extensions.iter().rev() {
            extension.after_pass(pass.name(), &before, &after, ctx)?;
        }

        Ok(after)
    }

    /// Runs an async optimizer phase and invokes registered extensions around it.
    pub async fn run_async<F, Fut>(
        &self,
        pass_name: &'static str,
        root: Arc<Operator>,
        ctx: &IRContext,
        run: F,
    ) -> Result<Arc<Operator>>
    where
        F: FnOnce(Arc<Operator>) -> Fut,
        Fut: Future<Output = Result<Arc<Operator>>>,
    {
        for extension in self.extensions.iter() {
            extension.before_pass(pass_name, &root, ctx)?;
        }

        let before = root.clone();
        let after = run(root).await?;

        for extension in self.extensions.iter().rev() {
            extension.after_pass(pass_name, &before, &after, ctx)?;
        }

        Ok(after)
    }

    #[doc(hidden)]
    /// Returns the number of registered extensions.
    ///
    /// This is primarily used by tests to verify pass-manager configuration.
    pub fn extension_count(&self) -> usize {
        self.extensions.len()
    }
}

impl Default for PassManager {
    fn default() -> Self {
        PassManagerBuilder::default().build()
    }
}

/// Builder for a [`PassManager`].
#[derive(Default)]
pub struct PassManagerBuilder {
    /// Extensions that will be registered in insertion order.
    extensions: Vec<Arc<dyn PassExtension>>,
}

impl PassManagerBuilder {
    /// Registers a pass extension.
    pub fn add_extension(mut self, extension: impl PassExtension + 'static) -> Self {
        self.extensions.push(Arc::new(extension));
        self
    }

    /// Registers a shared pass extension.
    pub fn add_extension_arc(mut self, extension: Arc<dyn PassExtension>) -> Self {
        self.extensions.push(extension);
        self
    }

    /// Builds the pass manager.
    pub fn build(self) -> PassManager {
        PassManager {
            extensions: self.extensions.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PassExtension, PassManager, PlanPass};
    use crate::{
        error::Result,
        ir::{IRContext, Operator, table_ref::TableRef, test_utils::test_ctx_with_tables},
    };
    use std::sync::{Arc, Mutex};

    struct NoopPass;

    impl PlanPass for NoopPass {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn run(&self, root: Arc<Operator>, _ctx: &IRContext) -> Result<Arc<Operator>> {
            Ok(root)
        }
    }

    #[derive(Default)]
    struct RecordingExtension {
        events: Mutex<Vec<String>>,
    }

    impl RecordingExtension {
        fn recorded(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl PassExtension for RecordingExtension {
        fn before_pass(
            &self,
            pass_name: &'static str,
            _root: &Arc<Operator>,
            _ctx: &IRContext,
        ) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("before:{pass_name}"));
            Ok(())
        }

        fn after_pass(
            &self,
            pass_name: &'static str,
            _before: &Arc<Operator>,
            _after: &Arc<Operator>,
            _ctx: &IRContext,
        ) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("after:{pass_name}"));
            Ok(())
        }
    }

    #[test]
    fn pass_manager_runs_extensions_around_pass() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 1)])?;
        let plan = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let extension = Arc::new(RecordingExtension::default());
        let manager = PassManager::builder()
            .add_extension_arc(extension.clone())
            .build();

        let rewritten = manager.run(&NoopPass, plan.clone(), &ctx)?;

        assert_eq!(rewritten, plan);
        assert_eq!(extension.recorded(), vec!["before:noop", "after:noop"]);
        Ok(())
    }

    #[test]
    fn pass_manager_unwinds_after_hooks_in_reverse_registration_order() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 1)])?;
        let plan = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let events = Arc::new(Mutex::new(Vec::new()));

        struct SharedRecordingExtension {
            name: &'static str,
            events: Arc<Mutex<Vec<String>>>,
        }

        impl PassExtension for SharedRecordingExtension {
            fn before_pass(
                &self,
                pass_name: &'static str,
                _root: &Arc<Operator>,
                _ctx: &IRContext,
            ) -> Result<()> {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("before:{}:{pass_name}", self.name));
                Ok(())
            }

            fn after_pass(
                &self,
                pass_name: &'static str,
                _before: &Arc<Operator>,
                _after: &Arc<Operator>,
                _ctx: &IRContext,
            ) -> Result<()> {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("after:{}:{pass_name}", self.name));
                Ok(())
            }
        }

        let manager = PassManager::builder()
            .add_extension_arc(Arc::new(SharedRecordingExtension {
                name: "first",
                events: events.clone(),
            }))
            .add_extension_arc(Arc::new(SharedRecordingExtension {
                name: "second",
                events: events.clone(),
            }))
            .build();

        let rewritten = manager.run(&NoopPass, plan.clone(), &ctx)?;

        assert_eq!(rewritten, plan);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "before:first:noop",
                "before:second:noop",
                "after:second:noop",
                "after:first:noop",
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn pass_manager_runs_extensions_around_async_phase() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 1)])?;
        let plan = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let extension = Arc::new(RecordingExtension::default());
        let manager = PassManager::builder()
            .add_extension_arc(extension.clone())
            .build();

        let rewritten = manager
            .run_async(
                "async-noop",
                plan.clone(),
                &ctx,
                |root| async move { Ok(root) },
            )
            .await?;

        assert_eq!(rewritten, plan);
        assert_eq!(
            extension.recorded(),
            vec!["before:async-noop", "after:async-noop"]
        );
        Ok(())
    }
}
