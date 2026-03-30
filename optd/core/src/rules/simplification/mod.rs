mod pass;
mod prune;
mod rewrite;
mod rule;
mod scalar;

pub use pass::SimplificationPass;

#[cfg(test)]
mod tests;
