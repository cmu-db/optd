use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use std::collections::BTreeMap;

/// Generic display tree node that can be rendered independently of query plans.
#[derive(Debug, Clone)]
pub struct DisplayNode {
    pub kind: String,
    pub title: String,
    pub fields: Vec<DisplayField>,
    pub inputs: Vec<DisplayInput>,
    pub metadata: BTreeMap<String, DisplayValue>,
}

impl Serialize for DisplayNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(
            2 + self.fields.len() + self.inputs.len() + self.metadata.len(),
        ))?;
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
            kind: title.clone(),
            title,
            fields: Vec::new(),
            inputs: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_kind(kind: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            title: title.into(),
            fields: Vec::new(),
            inputs: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push(DisplayField {
            key: key.into(),
            value: DisplayValue::Scalar(value.into()),
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
            .insert(key.into(), DisplayValue::Scalar(value.into()));
        self
    }
}

/// Ordered key-value field for a [`DisplayNode`].
#[derive(Debug, Clone, Serialize)]
pub struct DisplayField {
    pub key: String,
    pub value: DisplayValue,
}

/// Value for a [`DisplayField`].
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum DisplayValue {
    Scalar(String),
    List(Vec<String>),
}

/// Flat display plan for stable serde output.
#[derive(Debug, Clone, Serialize)]
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
}

impl Serialize for DisplayNodeRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3 + self.fields.len() + self.inputs.len()))?;
        map.serialize_entry("id", &self.id)?;
        serialize_display_header(&mut map, &self.kind, &self.title)?;
        serialize_display_properties(&mut map, &self.fields)?;
        serialize_display_properties(&mut map, &self.inputs)?;
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

    pub fn insert(&mut self, order: usize, key: impl Into<String>, value: V) {
        self.entries.insert((order, key.into()), value);
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.entries
            .iter()
            .map(|((_order, key), value)| (key.as_str(), value))
    }
}

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
#[derive(Debug, Clone, Serialize)]
pub struct DisplayInput {
    pub name: String,
    pub node: Box<DisplayNode>,
}

fn serialize_display_header<M>(map: &mut M, kind: &str, title: &str) -> Result<(), M::Error>
where
    M: SerializeMap,
{
    map.serialize_entry("kind", kind)?;
    map.serialize_entry("title", title)
}

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
}

impl Default for BoxRendererConfig {
    fn default() -> Self {
        Self {
            min_box_width: 12,
            max_box_width: 80,
            child_gap: 4,
        }
    }
}

/// Renders a generic [`DisplayNode`] tree using box drawing characters.
pub struct BoxDrawingRenderer {
    config: BoxRendererConfig,
}

impl Default for BoxDrawingRenderer {
    fn default() -> Self {
        Self {
            config: BoxRendererConfig::default(),
        }
    }
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
        let parent = self.render_box(&node.title, &self.render_details(node));

        match node.inputs.as_slice() {
            [] => parent,
            [input] => self.render_unary_node(parent, input),
            [outer, inner] => self.render_binary_node(parent, outer, inner),
            inputs => self.render_nary_node(parent, inputs),
        }
    }

    fn render_details(&self, node: &DisplayNode) -> Vec<String> {
        let fields = node
            .fields
            .iter()
            .flat_map(|field| self.render_field(field));
        let metadata = node
            .metadata
            .iter()
            .flat_map(|(key, value)| self.render_entry(key, value));

        fields.chain(metadata).collect()
    }

    fn render_field(&self, field: &DisplayField) -> Vec<String> {
        self.render_entry(&field.key, &field.value)
    }

    fn render_entry(&self, key: &str, value: &DisplayValue) -> Vec<String> {
        match value {
            DisplayValue::Scalar(value) => vec![format!("{key}: {value}")],
            DisplayValue::List(values) => {
                let mut details = Vec::with_capacity(values.len() + 1);
                details.push(format!("{key}:"));
                details.extend(values.iter().map(|value| format!("  {value}")));
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
            "{:<left_width$}{}{}",
            format!("│ {}", outer.name),
            " ".repeat(gap),
            format!("│ {}", inner.name),
            left_width = left_width
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
                "{outer_line:<left_width$}{}{inner_line}",
                " ".repeat(gap),
                left_width = left_width
            ));
        }

        RenderedBlock { lines, width }
    }

    fn render_nary_node(&self, parent: RenderedBlock, inputs: &[DisplayInput]) -> RenderedBlock {
        let mut width = parent.width;
        let mut lines = parent.lines;

        for input in inputs {
            let child = self.render_node(&input.node);
            width = width.max(child.width);
            lines.push(format!("│ {}", input.name));
            lines.extend(child.lines);
        }

        RenderedBlock { lines, width }
    }

    fn render_box(&self, title: &str, details: &[String]) -> RenderedBlock {
        let max_box_width = self.config.max_box_width.max(4);
        let max_content_width = max_box_width.saturating_sub(4).max(1);
        let wrapped_details = self.wrap_details(details, max_content_width);
        let content_width = wrapped_details
            .iter()
            .map(|detail| detail.chars().count())
            .chain(std::iter::once(title.chars().count()))
            .max()
            .unwrap_or(0)
            .max(self.config.min_box_width)
            .min(max_content_width);

        let mut lines = Vec::with_capacity(wrapped_details.len() + 4);
        lines.push(format!("┌{}┐", "─".repeat(content_width + 2)));
        lines.push(format!("│ {title:content_width$} │"));
        lines.push(format!("├{}┤", "─".repeat(content_width + 2)));

        for detail in wrapped_details {
            lines.push(format!("│ {detail:content_width$} │"));
        }

        lines.push(format!("└{}┘", "─".repeat(content_width + 2)));

        RenderedBlock {
            width: content_width + 4,
            lines,
        }
    }

    fn wrap_details(&self, details: &[String], max_width: usize) -> Vec<String> {
        details
            .iter()
            .flat_map(|detail| self.wrap_detail(detail, max_width))
            .collect()
    }

    fn wrap_detail(&self, detail: &str, max_width: usize) -> Vec<String> {
        if detail.chars().count() <= max_width {
            return vec![detail.to_string()];
        }

        let requested_indent_width = detail.chars().take_while(|ch| ch.is_whitespace()).count();
        let indent_width = requested_indent_width.min(max_width.saturating_sub(1));
        let indent = " ".repeat(indent_width);
        let body = detail.trim_start();
        let max_body_width = max_width - indent_width;
        let mut lines = Vec::new();
        let mut current = String::new();

        for word in body.split_whitespace() {
            let separator_width = usize::from(!current.is_empty());
            let next_width = current.chars().count() + separator_width + word.chars().count();

            if !current.is_empty() && next_width > max_body_width {
                lines.push(format!("{indent}{current}"));
                current = String::new();
            }

            if word.chars().count() > max_body_width {
                if !current.is_empty() {
                    lines.push(format!("{indent}{current}"));
                    current = String::new();
                }

                lines.extend(
                    self.wrap_long_word(word, max_body_width)
                        .into_iter()
                        .map(|line| format!("{indent}{line}")),
                );
                continue;
            }

            if !current.is_empty() {
                current.push(' ');
            }

            current.push_str(word);
        }

        if !current.is_empty() {
            lines.push(format!("{indent}{current}"));
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
