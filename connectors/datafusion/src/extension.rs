use datafusion::common::{config::ConfigExtension, extensions_options};

extensions_options! {
   /// optd configuration in datafusion.
   pub struct OptdExtensionConfig {
       /// Should try run optd optimizer instead of datafusion default.
       pub optd_enabled: bool, default = true
   }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}

/// The optd datafusion extension used to store shared state.
#[derive(Debug)]
pub struct OptdExtension;
