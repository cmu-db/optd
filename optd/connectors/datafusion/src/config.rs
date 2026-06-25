use datafusion::common::{config::ConfigExtension, extensions_options};

extensions_options! {
    /// Configuration for optd optimizer behavior in DataFusion sessions.
    pub struct OptdExtensionConfig {
        /// Whether SQL execution should route through optd before DataFusion.
        pub optd_enabled: bool, default = true

        /// Whether explain-step logging should be enabled by default.
        pub log_explain_steps: bool, default = true

        /// Whether optd should execute optimized IR through direct DataFusion physical planning.
        pub physical_planning: bool, default = true
    }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}
