#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "serde")]
use serde::ser::{SerializeMap, Serializer};
use std::collections::BTreeMap;
use std::io::IsTerminal;

/// Generic display tree node that can be rendered independently of query plans.
#[derive(Debug, Clone)]
pub struct DisplayNode {
    pub id: Option<String>,
    pub kind: String,
    pub title: String,
    pub fields: Vec<DisplayField>,
    pub inputs: Vec<DisplayInput>,
    pub metadata: BTreeMap<String, DisplayValue>,
}

#[cfg(feature = "serde")]
impl Serialize for DisplayNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(
            2 + usize::from(self.id.is_some())
                + self.fields.len()
                + self.inputs.len()
                + self.metadata.len(),
        ))?;
        if let Some(id) = &self.id {
            map.serialize_entry("id", id)?;
        }
        serialize_display_header(&mut map, &self.kind, &self.title)?;
        serialize_display_fields(&mut map, self.fields.iter())?;
        serialize_display_inputs(&mut map, self.inputs.iter())?;
        serialize_display_metadata(&mut map, &self.metadata)?;
        map.end()
    }
}

impl DisplayNode {
    pub fn new(title: impl Into<String>) -> Self {
        let title = title.into();
        Self {
            id: None,
            kind: title.clone(),
            title,
            fields: Vec::new(),
            inputs: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_kind(kind: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            id: None,
            kind: kind.into(),
            title: title.into(),
            fields: Vec::new(),
            inputs: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_id(mut self, id: impl std::fmt::Display) -> Self {
        self.id = Some(id.to_string());
        self
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push(DisplayField {
            key: key.into(),
            value: DisplayValue::Atom(value.into()),
        });
        self
    }

    pub fn with_list_field<I, S>(mut self, key: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields.push(DisplayField {
            key: key.into(),
            value: DisplayValue::List(values.into_iter().map(Into::into).collect()),
        });
        self
    }

    pub fn with_input(mut self, name: impl Into<String>, node: DisplayNode) -> Self {
        self.inputs.push(DisplayInput {
            name: name.into(),
            node: Box::new(node),
        });
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata
            .insert(key.into(), DisplayValue::Atom(value.into()));
        self
    }
}

/// Ordered key-value field for a [`DisplayNode`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DisplayField {
    pub key: String,
    pub value: DisplayValue,
}

/// Value for a [`DisplayField`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum DisplayValue {
    Atom(String),
    List(Vec<String>),
}

/// Flat display plan for stable serde output.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DisplayPlan {
    pub nodes: Vec<DisplayNodeRecord>,
}

/// Stable flat display record for one operator-like node.
#[derive(Debug, Clone)]
pub struct DisplayNodeRecord {
    pub id: usize,
    pub kind: String,
    pub title: String,
    pub fields: DisplayProperties<DisplayValue>,
    pub inputs: DisplayProperties<usize>,
    pub metadata: DisplayProperties<DisplayValue>,
}

#[cfg(feature = "serde")]
impl Serialize for DisplayNodeRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(
            3 + self.fields.len() + self.inputs.len() + self.metadata.len(),
        ))?;
        map.serialize_entry("id", &self.id)?;
        serialize_display_header(&mut map, &self.kind, &self.title)?;
        serialize_display_properties(&mut map, &self.fields)?;
        serialize_display_properties(&mut map, &self.inputs)?;
        serialize_display_properties(&mut map, &self.metadata)?;
        map.end()
    }
}

/// Ordered display properties that serialize using only their string keys.
#[derive(Debug, Clone)]
pub struct DisplayProperties<V> {
    entries: BTreeMap<(usize, String), V>,
}

impl<V> Default for DisplayProperties<V> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<V> DisplayProperties<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn insert(&mut self, order: usize, key: impl Into<String>, value: V) {
        self.entries.insert((order, key.into()), value);
    }

    #[cfg(feature = "serde")]
    fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.entries
            .iter()
            .map(|((_order, key), value)| (key.as_str(), value))
    }
}

#[cfg(feature = "serde")]
impl<V> Serialize for DisplayProperties<V>
where
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        serialize_display_properties(&mut map, self)?;
        map.end()
    }
}

/// Named child input for a [`DisplayNode`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DisplayInput {
    pub name: String,
    pub node: Box<DisplayNode>,
}

/// optd optimizer visualizer pass wrapper.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(rename_all = "camelCase")
)]
pub struct OptimizerVisualizerPass {
    pub pass_name: String,
    pub root: OptimizerVisualizerNode,
}

/// optd optimizer visualizer node shape.
#[derive(Debug, Clone)]
pub struct OptimizerVisualizerNode {
    pub op: String,
    pub title: String,
    pub cost: f64,
    pub rows: f64,
    pub table: Option<String>,
    pub children: Vec<OptimizerVisualizerNode>,
    pub properties: BTreeMap<String, OptimizerVisualizerValue>,
}

/// Extra display property value for an optd optimizer visualizer node.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum OptimizerVisualizerValue {
    String(String),
    Strings(Vec<String>),
}

impl OptimizerVisualizerPass {
    pub fn new(pass_name: impl Into<String>, root: OptimizerVisualizerNode) -> Self {
        Self {
            pass_name: pass_name.into(),
            root,
        }
    }
}

impl OptimizerVisualizerNode {
    pub fn from_display_node(node: &DisplayNode) -> Self {
        let mut properties = BTreeMap::new();
        if let Some(id) = &node.id {
            properties.insert(
                "operator_id".to_string(),
                OptimizerVisualizerValue::String(id.clone()),
            );
        }
        for field in &node.fields {
            properties.insert(
                field.key.clone(),
                OptimizerVisualizerValue::from_display_value(&field.value),
            );
        }
        for (key, value) in &node.metadata {
            properties.insert(
                key.clone(),
                OptimizerVisualizerValue::from_display_value(value),
            );
        }

        Self {
            op: node.kind.clone(),
            title: node.title.clone(),
            cost: 0.0,
            rows: 0.0,
            table: display_node_table(node),
            children: node
                .inputs
                .iter()
                .map(|input| OptimizerVisualizerNode::from_display_node(&input.node))
                .collect(),
            properties,
        }
    }
}

impl OptimizerVisualizerValue {
    fn from_display_value(value: &DisplayValue) -> Self {
        match value {
            DisplayValue::Atom(value) => Self::String(value.clone()),
            DisplayValue::List(values) => Self::Strings(values.clone()),
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for OptimizerVisualizerNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 4 + self.properties.len();
        if self.table.is_some() {
            len += 1;
        }
        if !self.children.is_empty() {
            len += 1;
        }

        let mut map = serializer.serialize_map(Some(len))?;
        map.serialize_entry("op", &self.op)?;
        map.serialize_entry("title", &self.title)?;
        map.serialize_entry("cost", &self.cost)?;
        map.serialize_entry("rows", &self.rows)?;
        if let Some(table) = &self.table {
            map.serialize_entry("table", table)?;
        }
        if !self.children.is_empty() {
            map.serialize_entry("children", &self.children)?;
        }
        for (key, value) in &self.properties {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

fn display_node_table(node: &DisplayNode) -> Option<String> {
    if node.kind != "scan" {
        return None;
    }

    node.fields.iter().find_map(|field| {
        if field.key != "table_name" {
            return None;
        }

        match &field.value {
            DisplayValue::Atom(table) if !table.is_empty() => Some(table.clone()),
            _ => None,
        }
    })
}

#[cfg(feature = "serde")]
fn serialize_display_header<M>(map: &mut M, kind: &str, title: &str) -> Result<(), M::Error>
where
    M: SerializeMap,
{
    map.serialize_entry("kind", kind)?;
    map.serialize_entry("title", title)
}

#[cfg(feature = "serde")]
fn serialize_display_fields<'a, M, I>(map: &mut M, fields: I) -> Result<(), M::Error>
where
    M: SerializeMap,
    I: IntoIterator<Item = &'a DisplayField>,
{
    for field in fields {
        map.serialize_entry(&field.key, &field.value)?;
    }

    Ok(())
}

#[cfg(feature = "serde")]
fn serialize_display_inputs<'a, M, I>(map: &mut M, inputs: I) -> Result<(), M::Error>
where
    M: SerializeMap,
    I: IntoIterator<Item = &'a DisplayInput>,
{
    for input in inputs {
        map.serialize_entry(&input.name, &input.node)?;
    }

    Ok(())
}

#[cfg(feature = "serde")]
fn serialize_display_metadata<M>(
    map: &mut M,
    metadata: &BTreeMap<String, DisplayValue>,
) -> Result<(), M::Error>
where
    M: SerializeMap,
{
    for (key, value) in metadata {
        map.serialize_entry(key, value)?;
    }

    Ok(())
}

#[cfg(feature = "serde")]
fn serialize_display_properties<M, V>(
    map: &mut M,
    properties: &DisplayProperties<V>,
) -> Result<(), M::Error>
where
    M: SerializeMap,
    V: Serialize,
{
    for (key, value) in properties.iter() {
        map.serialize_entry(key, value)?;
    }

    Ok(())
}

/// Display settings for [`BoxDrawingRenderer`].
#[derive(Debug, Clone)]
pub struct BoxRendererConfig {
    pub min_box_width: usize,
    pub max_box_width: usize,
    pub child_gap: usize,
    pub color_mode: ColorMode,
    pub theme: BoxRendererTheme,
}

impl Default for BoxRendererConfig {
    fn default() -> Self {
        Self {
            min_box_width: 12,
            max_box_width: 80,
            child_gap: 4,
            color_mode: ColorMode::Never,
            theme: BoxRendererTheme::default(),
        }
    }
}

impl BoxRendererConfig {
    pub fn with_color_mode(mut self, color_mode: ColorMode) -> Self {
        self.color_mode = color_mode;
        self
    }
}

/// Controls ANSI color output for [`BoxDrawingRenderer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
    Auto,
    Always,
    Never,
}

/// ANSI styles used by [`BoxDrawingRenderer`].
#[derive(Debug, Clone)]
pub struct BoxRendererTheme {
    pub metadata_key: anstyle::Style,
    pub metadata_value: anstyle::Style,
}

impl Default for BoxRendererTheme {
    fn default() -> Self {
        Self {
            metadata_key: anstyle::Style::new()
                .fg_color(Some(anstyle::AnsiColor::Yellow.into()))
                .effects(anstyle::Effects::BOLD),
            metadata_value: anstyle::Style::new().fg_color(Some(anstyle::AnsiColor::Cyan.into())),
        }
    }
}

/// Renders a generic [`DisplayNode`] tree using box drawing characters.
#[derive(Default)]
pub struct BoxDrawingRenderer {
    config: BoxRendererConfig,
}

impl BoxDrawingRenderer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: BoxRendererConfig) -> Self {
        Self { config }
    }

    pub fn render(&self, node: &DisplayNode) -> String {
        self.render_node(node).to_string()
    }

    fn render_node(&self, node: &DisplayNode) -> RenderedBlock {
        let parent = self.render_box(&node.title, node.id.as_deref(), &self.render_details(node));

        match node.inputs.as_slice() {
            [] => parent,
            [input] => self.render_unary_node(parent, input),
            [outer, inner] => self.render_binary_node(parent, outer, inner),
            inputs => self.render_nary_node(parent, node.id.as_deref(), inputs),
        }
    }

    fn render_details(&self, node: &DisplayNode) -> Vec<RenderDetail> {
        let mut details = node
            .fields
            .iter()
            .flat_map(|field| self.render_field(field))
            .collect::<Vec<_>>();

        if !details.is_empty() && !node.metadata.is_empty() {
            details.push(RenderDetail::separator());
        }

        details.extend(
            node.metadata
                .iter()
                .flat_map(|(key, value)| self.render_metadata_entry(key, value)),
        );

        details
    }

    fn render_field(&self, field: &DisplayField) -> Vec<RenderDetail> {
        self.render_entry(&field.key, &field.value)
    }

    fn render_entry(&self, key: &str, value: &DisplayValue) -> Vec<RenderDetail> {
        match value {
            DisplayValue::Atom(value) => vec![RenderDetail::plain(format!("{key}: {value}"))],
            DisplayValue::List(values) => {
                let mut details = Vec::with_capacity(values.len() + 1);
                details.push(RenderDetail::plain(format!("{key}:")));
                details.extend(
                    values
                        .iter()
                        .map(|value| RenderDetail::plain(format!("  {value}"))),
                );
                details
            }
        }
    }

    fn render_metadata_entry(&self, key: &str, value: &DisplayValue) -> Vec<RenderDetail> {
        match value {
            DisplayValue::Atom(value) => {
                vec![RenderDetail::metadata_scalar(key, value)]
            }
            DisplayValue::List(values) => {
                let mut details = Vec::with_capacity(values.len() + 1);
                details.push(RenderDetail::metadata_key(format!("{key}:")));
                details.extend(
                    values
                        .iter()
                        .map(|value| RenderDetail::metadata_value(format!("  {value}"))),
                );
                details
            }
        }
    }

    fn render_unary_node(&self, parent: RenderedBlock, input: &DisplayInput) -> RenderedBlock {
        let child = self.render_node(&input.node);
        let width = parent.width.max(child.width);
        let mut lines = parent.lines;

        lines.push(format!("│ {}", input.name));
        lines.extend(child.lines);

        RenderedBlock { lines, width }
    }

    fn render_binary_node(
        &self,
        parent: RenderedBlock,
        outer: &DisplayInput,
        inner: &DisplayInput,
    ) -> RenderedBlock {
        let outer_block = self.render_node(&outer.node);
        let inner_block = self.render_node(&inner.node);
        let gap = self.config.child_gap;
        let left_width = parent.width.max(outer_block.width);
        let inner_start = left_width + gap;
        let branch_width = inner_start - parent.width;
        let width = left_width + gap + inner_block.width;
        let mut lines = parent.lines;

        if let Some(line) = lines.get_mut(1) {
            line.push_str(&"─".repeat(branch_width));
            line.push('┐');
        }
        for line in lines.iter_mut().skip(2) {
            line.push_str(&" ".repeat(branch_width));
            line.push('│');
        }

        lines.push(format!(
            "{}{}│ {}",
            pad_visible(&format!("│ {}", outer.name), left_width),
            " ".repeat(gap),
            inner.name
        ));

        let height = outer_block.lines.len().max(inner_block.lines.len());
        for index in 0..height {
            let outer_line = outer_block
                .lines
                .get(index)
                .map(String::as_str)
                .unwrap_or("");
            let inner_line = inner_block
                .lines
                .get(index)
                .map(String::as_str)
                .unwrap_or("");

            lines.push(format!(
                "{}{}{inner_line}",
                pad_visible(outer_line, left_width),
                " ".repeat(gap)
            ));
        }

        RenderedBlock { lines, width }
    }

    fn render_nary_node(
        &self,
        parent: RenderedBlock,
        parent_id: Option<&str>,
        inputs: &[DisplayInput],
    ) -> RenderedBlock {
        let mut width = parent.width;
        let mut lines = parent.lines;

        for (index, input) in inputs.iter().enumerate() {
            let child = self.render_node(&input.node);
            width = width.max(child.width);
            if index > 0
                && let Some(parent_id) = parent_id
            {
                let anchor = render_input_source_anchor(parent_id);
                width = width.max(anchor.width);
                lines.extend(anchor.lines);
            }
            lines.push(format!("│ {}", input.name));
            lines.extend(child.lines);
        }

        RenderedBlock { lines, width }
    }

    fn render_box(&self, title: &str, id: Option<&str>, details: &[RenderDetail]) -> RenderedBlock {
        let max_box_width = self.config.max_box_width.max(4);
        let max_content_width = max_box_width.saturating_sub(4).max(1);
        let wrapped_details = self.wrap_details(details, max_content_width);
        let title_width = title_content_width(title, id);
        let content_width = wrapped_details
            .iter()
            .map(|detail| detail.text.chars().count())
            .chain(std::iter::once(title_width))
            .max()
            .unwrap_or(0)
            .max(self.config.min_box_width)
            .min(max_content_width);

        let mut lines = Vec::with_capacity(wrapped_details.len() + 4);
        lines.push(format!("┌{}┐", "─".repeat(content_width + 2)));
        lines.push(format!(
            "│ {} │",
            render_title_line(title, id, content_width)
        ));
        lines.push(format!("├{}┤", "─".repeat(content_width + 2)));

        for detail in wrapped_details {
            if detail.is_separator() {
                lines.push(format!("│ {} │", "┄".repeat(content_width)));
                continue;
            }

            let padding = content_width.saturating_sub(detail.text.chars().count());
            lines.push(format!(
                "│ {}{} │",
                self.style_detail(&detail),
                " ".repeat(padding)
            ));
        }

        lines.push(format!("└{}┘", "─".repeat(content_width + 2)));

        RenderedBlock {
            width: content_width + 4,
            lines,
        }
    }

    fn wrap_details(&self, details: &[RenderDetail], max_width: usize) -> Vec<RenderDetail> {
        details
            .iter()
            .flat_map(|detail| self.wrap_detail(detail, max_width))
            .collect()
    }

    fn wrap_detail(&self, detail: &RenderDetail, max_width: usize) -> Vec<RenderDetail> {
        if detail.text.chars().count() <= max_width {
            return vec![detail.clone()];
        }

        if let RenderDetailStyle::MetadataScalar { key, value } = &detail.style {
            let expanded = [
                RenderDetail::metadata_key(format!("{key}:")),
                RenderDetail::metadata_value(format!("  {value}")),
            ];
            return self.wrap_details(&expanded, max_width);
        }

        let requested_indent_width = detail
            .text
            .chars()
            .take_while(|ch| ch.is_whitespace())
            .count();
        let indent_width = requested_indent_width.min(max_width.saturating_sub(1));
        let indent = " ".repeat(indent_width);
        let body = detail.text.trim_start();
        let max_body_width = max_width - indent_width;
        let mut lines = Vec::new();
        let mut current = String::new();

        for word in body.split_whitespace() {
            let separator_width = usize::from(!current.is_empty());
            let next_width = current.chars().count() + separator_width + word.chars().count();

            if !current.is_empty() && next_width > max_body_width {
                lines.push(detail.with_text(format!("{indent}{current}")));
                current = String::new();
            }

            if word.chars().count() > max_body_width {
                if !current.is_empty() {
                    lines.push(detail.with_text(format!("{indent}{current}")));
                    current = String::new();
                }

                lines.extend(
                    self.wrap_long_word(word, max_body_width)
                        .into_iter()
                        .map(|line| detail.with_text(format!("{indent}{line}"))),
                );
                continue;
            }

            if !current.is_empty() {
                current.push(' ');
            }

            current.push_str(word);
        }

        if !current.is_empty() {
            lines.push(detail.with_text(format!("{indent}{current}")));
        }

        lines
    }

    fn wrap_long_word(&self, word: &str, max_width: usize) -> Vec<String> {
        let mut lines = Vec::new();
        let mut current = String::new();

        for ch in word.chars() {
            if current.chars().count() == max_width {
                lines.push(current);
                current = String::new();
            }

            current.push(ch);
        }

        if !current.is_empty() {
            lines.push(current);
        }

        lines
    }

    fn style_detail(&self, detail: &RenderDetail) -> String {
        if !self.use_color() {
            return detail.text.clone();
        }

        match &detail.style {
            RenderDetailStyle::Plain => detail.text.clone(),
            RenderDetailStyle::Separator => detail.text.clone(),
            RenderDetailStyle::MetadataKey => {
                self.apply_style(self.config.theme.metadata_key, &detail.text)
            }
            RenderDetailStyle::MetadataValue => {
                self.apply_style(self.config.theme.metadata_value, &detail.text)
            }
            RenderDetailStyle::MetadataScalar { key, value } => format!(
                "{}: {}",
                self.apply_style(self.config.theme.metadata_key, key),
                self.apply_style(self.config.theme.metadata_value, value)
            ),
        }
    }

    fn apply_style(&self, style: anstyle::Style, text: &str) -> String {
        format!("{}{text}{}", style.render(), style.render_reset())
    }

    fn use_color(&self) -> bool {
        match self.config.color_mode {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => std::io::stdout().is_terminal(),
        }
    }
}

#[derive(Debug, Clone)]
struct RenderDetail {
    text: String,
    style: RenderDetailStyle,
}

#[derive(Debug, Clone)]
enum RenderDetailStyle {
    Plain,
    Separator,
    MetadataKey,
    MetadataValue,
    MetadataScalar { key: String, value: String },
}

impl RenderDetail {
    fn plain(text: String) -> Self {
        Self {
            text,
            style: RenderDetailStyle::Plain,
        }
    }

    fn separator() -> Self {
        Self {
            text: String::new(),
            style: RenderDetailStyle::Separator,
        }
    }

    fn is_separator(&self) -> bool {
        matches!(self.style, RenderDetailStyle::Separator)
    }

    fn metadata_key(text: String) -> Self {
        Self {
            text,
            style: RenderDetailStyle::MetadataKey,
        }
    }

    fn metadata_value(text: String) -> Self {
        Self {
            text,
            style: RenderDetailStyle::MetadataValue,
        }
    }

    fn metadata_scalar(key: &str, value: &str) -> Self {
        Self {
            text: format!("{key}: {value}"),
            style: RenderDetailStyle::MetadataScalar {
                key: key.to_string(),
                value: value.to_string(),
            },
        }
    }

    fn with_text(&self, text: String) -> Self {
        Self {
            text,
            style: self.style.clone(),
        }
    }
}

fn pad_visible(text: &str, target_width: usize) -> String {
    let width = visible_width(text);
    if width >= target_width {
        return text.to_string();
    }

    format!("{text}{}", " ".repeat(target_width - width))
}

fn visible_width(text: &str) -> usize {
    let mut width = 0;
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for ch in chars.by_ref() {
                if ch.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }

        width += 1;
    }

    width
}

fn title_content_width(title: &str, id: Option<&str>) -> usize {
    let title_width = title.chars().count();
    match id {
        Some(id) => title_width + 1 + id.chars().count(),
        None => title_width,
    }
}

fn render_title_line(title: &str, id: Option<&str>, width: usize) -> String {
    match id {
        Some(id) => {
            let title_width = title.chars().count();
            let id_width = id.chars().count();
            let padding = width.saturating_sub(title_width + id_width);
            format!("{title}{}{id}", " ".repeat(padding))
        }
        None => format!("{title:width$}"),
    }
}

fn render_input_source_anchor(parent_id: &str) -> RenderedBlock {
    let content_width = parent_id.chars().count().max(1);
    RenderedBlock {
        width: content_width + 4,
        lines: vec![
            format!("┌{}┐", "─".repeat(content_width + 2)),
            format!("│ {parent_id} │"),
            format!("└{}┘", "─".repeat(content_width + 2)),
        ],
    }
}

struct RenderedBlock {
    lines: Vec<String>,
    width: usize,
}

impl std::fmt::Display for RenderedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for line in &self.lines {
            writeln!(f, "{line}")?;
        }

        Ok(())
    }
}
