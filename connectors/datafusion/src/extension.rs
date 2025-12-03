use datafusion::common::{config::ConfigExtension, extensions_options};

extensions_options! {
    /// optd configuration in datafusion.
    pub struct OptdExtensionConfig {
        /// Should try run optd optimizer instead of datafusion default.
        pub optd_enabled: bool, default = true
        /// Should refresh memo for each query.
        pub optd_refresh_memo: bool, default = false
        /// Should apply predicate pushdown before search.
        pub optd_predicate_pushdown: bool, default = true
    }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}

/// The optd datafusion extension used to store shared state.
#[derive(Debug)]
pub struct OptdExtension;
