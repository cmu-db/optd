use pretty_xmlish::Pretty;

use crate::ir::IRContext;

pub trait Explain {
    fn explain<'a>(&self, ctx: &IRContext, option: &ExplainOption) -> Pretty<'a>;
}

#[derive(Default)]
pub struct ExplainOption {
    pub show_output_columns: bool,
}

pub fn quick_explain<T>(v: impl AsRef<T>, ctx: &IRContext) -> String
where
    T: Explain,
{
    let option = ExplainOption::default();
    let pretty = v.as_ref().explain(ctx, &option);
    let mut out = String::with_capacity(114514);
    let mut config = pretty_xmlish::PrettyConfig {
        width: 0,
        need_boundaries: true,
        ..Default::default()
    };
    config.unicode(&mut out, &pretty);
    out
}
