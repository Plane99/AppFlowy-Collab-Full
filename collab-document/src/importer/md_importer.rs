use crate::blocks::{Block, BlockType, DocumentData, DocumentMeta};
use crate::document_data::generate_id;
use crate::error::DocumentError;
use crate::importer::define::*;
use crate::importer::delta::Delta;
use crate::importer::util::*;
use markdown::mdast::AlignKind;
use markdown::{Constructs, ParseOptions, mdast, to_mdast};
use serde_json::Value;
use std::collections::HashMap;
use tracing::trace;

#[derive(Default)]
pub struct MDImporter {
  /// The parse options for the markdown parser.
  ///
  /// If not set, the default options will be used.
  /// The default parse options contain
  /// - Github Flavored Markdown (GFM) features.
  /// - math text, math flow, autolink features.
  /// - default Markdown features.
  pub parse_options: ParseOptions,
}

impl MDImporter {
  pub fn new(parse_options: Option<ParseOptions>) -> Self {
    let parse_options = parse_options.unwrap_or_else(|| ParseOptions {
      gfm_strikethrough_single_tilde: true,
      constructs: Constructs {
        math_text: true,
        math_flow: true,
        autolink: true,
        ..Constructs::gfm()
      },
      ..ParseOptions::gfm()
    });

    Self { parse_options }
  }

  pub fn import(&self, document_id: &str, md: String) -> Result<DocumentData, DocumentError> {
    let md_node =
      to_mdast(&md, &self.parse_options).map_err(|_| DocumentError::ParseMarkdownError)?;

    let mut document_data = DocumentData {
      page_id: document_id.to_string(),
      blocks: HashMap::new(),
      meta: DocumentMeta {
        children_map: HashMap::new(),
        text_map: Some(HashMap::new()),
      },
    };

    process_mdast_node(
      &mut document_data,
      &md_node,
      None,
      Some(document_id.to_string()),
      None,
      None,
      &self.parse_options,
    );

    Ok(document_data)
  }
}

struct NotionColumnsTableInfo<'a> {
  col_count: usize,
  body_rows: &'a [mdast::Node],
}

fn parse_notion_columns_table(table: &mdast::Table) -> Option<NotionColumnsTableInfo<'_>> {
  let mut rows = table.children.iter();
  let header = rows.next()?;
  let mdast::Node::TableRow(header) = header else {
    return None;
  };

  let col_count = header.children.len();
  if col_count < 2 {
    return None;
  }

  let header_empty = header.children.iter().all(|cell| {
    let mdast::Node::TableCell(cell) = cell else {
      return false;
    };
    is_table_cell_empty(cell)
  });
  if !header_empty {
    return None;
  }

  let body_rows = rows.as_slice();
  if body_rows.is_empty() {
    return None;
  }

  let all_body_empty = body_rows.iter().all(|row| {
    let mdast::Node::TableRow(row) = row else {
      return false;
    };
    if row.children.len() != col_count {
      return false;
    }
    row.children.iter().all(|cell| {
      let mdast::Node::TableCell(cell) = cell else {
        return false;
      };
      is_table_cell_empty(cell)
    })
  });

  if all_body_empty {
    if col_count < 6 {
      return None;
    }
  } else if col_count > 5 || body_rows.len() > 3 {
    return None;
  }

  Some(NotionColumnsTableInfo { col_count, body_rows })
}

fn is_table_cell_empty(cell: &mdast::TableCell) -> bool {
  if cell.children.is_empty() {
    return true;
  }
  let mut text = String::new();
  collect_cell_text(&cell.children, &mut text);
  text.trim().is_empty()
}

fn collect_cell_text(nodes: &[mdast::Node], out: &mut String) {
  for node in nodes {
    match node {
      mdast::Node::Text(t) => out.push_str(&t.value),
      mdast::Node::InlineCode(c) => out.push_str(&c.value),
      mdast::Node::InlineMath(m) => out.push_str(&m.value),
      mdast::Node::Strong(s) => collect_cell_text(&s.children, out),
      mdast::Node::Emphasis(e) => collect_cell_text(&e.children, out),
      mdast::Node::Delete(d) => collect_cell_text(&d.children, out),
      mdast::Node::Link(l) => collect_cell_text(&l.children, out),
      _ => {},
    }
  }
}

/// This function will recursively process the mdast node and convert it to document blocks
/// The document blocks will be stored in the document data
fn process_mdast_node(
  document_data: &mut DocumentData,
  node: &mdast::Node,
  parent_id: Option<String>,
  block_id: Option<String>,
  list_type: Option<&str>,
  start_number: Option<u32>,
  parse_options: &ParseOptions,
) {
  // If the node is an inline node, process it as an inline node
  if is_inline_node(node) {
    trace!("Processing inline node: {:?}", node);
    process_inline_mdast_node(document_data, node, parent_id);
    return;
  }

  if let mdast::Node::Html(html) = node {
    let value = html.value.trim();
    if value == "</aside>" || value == "</details>" {
      return;
    }
  }

  trace!("Processing node: {:?}", node);
  // If the node is a list node, process it as a list node
  if let Some((children, list_type, start_number)) = get_mdast_node_info(node) {
    process_mdast_node_children(
      document_data,
      parent_id,
      children,
      Some(&list_type),
      start_number,
      parse_options,
    );
    return;
  }

  // flatten the image node, by default, the image is wrapped in a paragraph
  if let mdast::Node::Paragraph(para) = node {
    if para.children.len() == 1 && matches!(para.children[0], mdast::Node::Image(_)) {
      if let mdast::Node::Image(image) = &para.children[0] {
        if let Some(parent_id) = parent_id {
          return process_image(document_data, image, &parent_id);
        }
      }
    }
  }

  // Handle direct image nodes without creating intermediate blocks
  if let mdast::Node::Image(image) = node {
    if let Some(parent_id) = parent_id {
      return process_image(document_data, image, &parent_id);
    }
  }

  if let mdast::Node::Table(table) = node {
    if let Some(info) = parse_notion_columns_table(table) {
      let id = block_id.unwrap_or_else(generate_id);
      let block = Block {
        id: id.clone(),
        ty: BlockType::SimpleColumns.to_string(),
        data: BlockData::new(),
        parent: parent_id.clone().unwrap_or_default(),
        children: id.clone(),
        external_id: Some(id.clone()),
        external_type: Some("text".to_string()),
      };
      document_data.blocks.insert(id.clone(), block);
      ensure_children_map_entry(document_data, &id);
      update_children_map(document_data, parent_id.clone(), &id);

      for col_index in 0..info.col_count {
        let column_id = generate_id();
        let column_block = Block {
          id: column_id.clone(),
          ty: BlockType::SimpleColumn.to_string(),
          data: BlockData::new(),
          parent: id.clone(),
          children: column_id.clone(),
          external_id: Some(column_id.clone()),
          external_type: Some("text".to_string()),
        };
        document_data.blocks.insert(column_id.clone(), column_block);
        ensure_children_map_entry(document_data, &column_id);
        update_children_map(document_data, Some(id.clone()), &column_id);

        for row in info.body_rows.iter() {
          let mdast::Node::TableRow(row) = row else {
            continue;
          };
          let Some(mdast::Node::TableCell(cell)) = row.children.get(col_index) else {
            continue;
          };
          if is_table_cell_empty(cell) {
            continue;
          }

          let paragraph_block_id = create_paragraph_block(document_data, &column_id);
          process_mdast_node_children(
            document_data,
            Some(paragraph_block_id),
            &cell.children,
            None,
            None,
            parse_options,
          );
        }
      }
      return;
    }
  }

  // Process other nodes as normal nodes
  let id = block_id.unwrap_or_else(generate_id);

  let block = create_block(&id, node, parent_id.clone(), list_type, start_number);

  document_data.blocks.insert(id.clone(), block);
  ensure_children_map_entry(document_data, &id);

  update_children_map(document_data, parent_id, &id);

  match node {
    mdast::Node::Root(root) => {
      process_mdast_node_children(
        document_data,
        Some(id.clone()),
        &root.children,
        None,
        start_number,
        parse_options,
      );
    },
    mdast::Node::Paragraph(para) => {
      // Process paragraph as before
      process_mdast_node_children(
        document_data,
        Some(id.clone()),
        &para.children,
        None,
        start_number,
        parse_options,
      );
    },
    mdast::Node::Heading(heading) => {
      process_mdast_node_children(
        document_data,
        Some(id.clone()),
        &heading.children,
        None,
        start_number,
        parse_options,
      );
    },
    // handle the blockquote and list item node
    mdast::Node::Blockquote(_) | mdast::Node::ListItem(_) => {
      if let Some(children) = get_mdast_node_children(node) {
        if children.is_empty() {
          return;
        }

        if let Some((first, rest)) = children.split_first() {
          // use the first node as the content of the block
          if let mdast::Node::Paragraph(para) = first {
            process_mdast_node_children(
              document_data,
              Some(id.clone()),
              &para.children,
              None,
              start_number,
              parse_options,
            );
          }

          // continue to process the rest of the nodes
          process_mdast_node_children(
            document_data,
            Some(id.clone()),
            rest,
            list_type,
            start_number,
            parse_options,
          );
        }
      }
    },
    mdast::Node::Code(code) => {
      let mut delta = Delta::new();
      delta.insert(code.value.clone(), Vec::new());
      insert_delta_to_text_map(document_data, &id, delta);
    },
    mdast::Node::Table(table) => {
      // Process each row and create SimpleTableRow blocks
      for (row_index, row) in table.children.iter().enumerate() {
        if let mdast::Node::TableRow(row_node) = row {
          process_table_row(
            document_data,
            row_node,
            row_index,
            &id,
            &table.align,
            parse_options,
          );
        }
      }
    },
    // Image nodes are now handled earlier, so this case should not be reached
    mdast::Node::Image(_) => {
      // This should not be reached due to early return above
      unreachable!("Image nodes should be handled earlier");
    },
    _ => {
      trace!("Unhandled node: {:?}", node);
      // Default to processing as paragraph
      let children = node.to_string();
      let mut delta = Delta::new();
      delta.insert(children, Vec::new());
      insert_delta_to_text_map(document_data, &id, delta);
    },
  }
}

fn create_block(
  id: &str,
  node: &mdast::Node,
  parent_id: Option<String>,
  list_type: Option<&str>,
  start_number: Option<u32>,
) -> Block {
  Block {
    id: id.to_string(),
    ty: mdast_node_type_to_block_type(node, list_type),
    data: mdast_node_to_block_data(node, start_number),
    parent: parent_id.unwrap_or_default(),
    children: id.to_string(),
    external_id: Some(id.to_string()),
    external_type: Some("text".to_string()),
  }
}

fn update_children_map(
  document_data: &mut DocumentData,
  parent_id: Option<String>,
  child_id: &str,
) {
  if let Some(parent) = parent_id {
    document_data
      .meta
      .children_map
      .entry(parent)
      .or_default()
      .push(child_id.to_string());
  }
}

fn ensure_children_map_entry(document_data: &mut DocumentData, block_id: &str) {
  document_data
    .meta
    .children_map
    .entry(block_id.to_string())
    .or_default();
}

fn process_image(document_data: &mut DocumentData, image: &mdast::Image, parent_id: &str) {
  let new_block_id = generate_id();
  let image_block = create_image_block(&new_block_id, image.url.clone(), parent_id);
  document_data
    .blocks
    .insert(new_block_id.clone(), image_block);
  ensure_children_map_entry(document_data, &new_block_id);
  update_children_map(document_data, Some(parent_id.to_string()), &new_block_id);
}

fn process_table_row(
  document_data: &mut DocumentData,
  row_node: &mdast::TableRow,
  row_index: usize,
  table_id: &str,
  align: &[AlignKind],
  parse_options: &ParseOptions,
) {
  let row_id = generate_id();
  let row_block = create_simple_table_row_block(&row_id, table_id);
  document_data.blocks.insert(row_id.clone(), row_block);
  ensure_children_map_entry(document_data, &row_id);
  update_children_map(document_data, Some(table_id.to_string()), &row_id);

  for (col_index, cell) in row_node.children.iter().enumerate() {
    if let mdast::Node::TableCell(cell_node) = cell {
      let cell_id = generate_id();
      let cell_block =
        create_simple_table_cell_block(&cell_id, &row_id, row_index, col_index, align);
      document_data.blocks.insert(cell_id.clone(), cell_block);
      ensure_children_map_entry(document_data, &cell_id);
      update_children_map(document_data, Some(row_id.to_string()), &cell_id);

      let paragraph_block_id = create_paragraph_block(document_data, &cell_id);

      process_mdast_node_children(
        document_data,
        Some(paragraph_block_id.clone()),
        &cell_node.children,
        None,
        None,
        parse_options,
      );
    }
  }
}

fn create_paragraph_block(document_data: &mut DocumentData, parent_id: &str) -> String {
  let paragraph_node = mdast::Node::Paragraph(mdast::Paragraph {
    children: Vec::new(),
    position: None,
  });

  let paragraph_block_id = generate_id();
  let paragraph_block = create_block(
    &paragraph_block_id,
    &paragraph_node,
    Some(parent_id.to_string()),
    None,
    None,
  );

  document_data
    .blocks
    .insert(paragraph_block_id.clone(), paragraph_block);
  ensure_children_map_entry(document_data, &paragraph_block_id);
  update_children_map(
    document_data,
    Some(parent_id.to_string()),
    &paragraph_block_id,
  );

  paragraph_block_id
}

pub fn create_image_block(block_id: &str, url: String, parent_id: &str) -> Block {
  let mut data = BlockData::new();
  data.insert(URL_FIELD.to_string(), url.into());
  data.insert(IMAGE_TYPE_FIELD.to_string(), EXTERNAL_IMAGE_TYPE.into());
  Block {
    id: block_id.to_string(),
    ty: BlockType::Image.to_string(),
    data,
    parent: parent_id.to_string(),
    children: "".to_string(),
    external_id: None,
    external_type: None,
  }
}

fn create_simple_table_row_block(id: &str, parent_id: &str) -> Block {
  Block {
    id: id.to_string(),
    ty: BlockType::SimpleTableRow.to_string(),
    data: HashMap::new(),
    parent: parent_id.to_string(),
    children: id.to_string(),
    external_id: None,
    external_type: None,
  }
}

fn create_simple_table_cell_block(
  id: &str,
  parent_id: &str,
  row: usize,
  col: usize,
  alignments: &[AlignKind],
) -> Block {
  let mut cell_data = HashMap::new();
  cell_data.insert(ROW_POSITION_FIELD.to_string(), row.into());
  cell_data.insert(COL_POSITION_FIELD.to_string(), col.into());

  if let Some(align) = alignments.get(col) {
    let align_str = match align {
      AlignKind::Left => ALIGN_LEFT,
      AlignKind::Right => ALIGN_RIGHT,
      AlignKind::Center => ALIGN_CENTER,
      _ => ALIGN_LEFT,
    };
    cell_data.insert(
      ALIGN_FIELD.to_string(),
      Value::String(align_str.to_string()),
    );
  }

  Block {
    id: id.to_string(),
    ty: BlockType::SimpleTableCell.to_string(),
    data: cell_data,
    parent: parent_id.to_string(),
    children: id.to_string(),
    external_id: None,
    external_type: None,
  }
}

fn process_mdast_node_children(
  document_data: &mut DocumentData,
  parent_id: Option<String>,
  children: &[mdast::Node],
  list_type: Option<&str>,
  start_number: Option<u32>,
  parse_options: &ParseOptions,
) {
  let mut idx = 0;
  while idx < children.len() {
    if let mdast::Node::Html(html) = &children[idx] {
      let value = html.value.trim();
      if value == "</aside>" || value == "</details>" {
        idx += 1;
        continue;
      }

      if let Some(callout) = parse_aside_html(value) {
        let callout_id = generate_id();
        let mut data = BlockData::new();
        if !callout.icon.is_empty() {
          data.insert("icon".to_string(), callout.icon.into());
        }

        let block = Block {
          id: callout_id.clone(),
          ty: BlockType::Callout.to_string(),
          data,
          parent: parent_id.clone().unwrap_or_default(),
          children: callout_id.clone(),
          external_id: Some(callout_id.clone()),
          external_type: Some("text".to_string()),
        };
        document_data.blocks.insert(callout_id.clone(), block);
        update_children_map(document_data, parent_id.clone(), &callout_id);

        insert_markdown_as_inline_delta(document_data, &callout_id, &callout.content, parse_options);

        idx += 1;
        while idx < children.len() {
          if let mdast::Node::Html(h) = &children[idx] {
            if h.value.trim() == "</aside>" {
              idx += 1;
              break;
            }
          }

          process_mdast_node(
            document_data,
            &children[idx],
            Some(callout_id.clone()),
            None,
            list_type,
            start_number,
            parse_options,
          );
          idx += 1;
        }
        continue;
      }

      if value.starts_with("<details>") {
        let toggle_id = generate_id();
        let block = Block {
          id: toggle_id.clone(),
          ty: BlockType::ToggleList.to_string(),
          data: BlockData::new(),
          parent: parent_id.clone().unwrap_or_default(),
          children: toggle_id.clone(),
          external_id: Some(toggle_id.clone()),
          external_type: Some("text".to_string()),
        };
        document_data.blocks.insert(toggle_id.clone(), block);
        update_children_map(document_data, parent_id.clone(), &toggle_id);

        let mut summary_written = false;
        if let Some(details) = parse_details_html(value) {
          insert_markdown_as_inline_delta(document_data, &toggle_id, &details.summary, parse_options);
          summary_written = true;

          if !details.body.trim().is_empty() {
            if let Ok(inner_node) = to_mdast(&details.body, parse_options) {
              if let mdast::Node::Root(root) = inner_node {
                process_mdast_node_children(
                  document_data,
                  Some(toggle_id.clone()),
                  &root.children,
                  None,
                  None,
                  parse_options,
                );
              }
            }
          }
        }

        idx += 1;
        while idx < children.len() {
          if let mdast::Node::Html(h) = &children[idx] {
            let v = h.value.trim();
            if v == "</details>" {
              idx += 1;
              break;
            }

            if !summary_written && v.starts_with("<summary>") {
              if let Some((summary, rest)) = extract_tag_content(v, "summary") {
                insert_markdown_as_inline_delta(document_data, &toggle_id, &summary, parse_options);
                summary_written = true;

                let body = rest.trim();
                if !body.is_empty() {
                  if let Ok(inner_node) = to_mdast(body, parse_options) {
                    if let mdast::Node::Root(root) = inner_node {
                      process_mdast_node_children(
                        document_data,
                        Some(toggle_id.clone()),
                        &root.children,
                        None,
                        None,
                        parse_options,
                      );
                    }
                  }
                }
              }
              idx += 1;
              continue;
            }
          }

          process_mdast_node(
            document_data,
            &children[idx],
            Some(toggle_id.clone()),
            None,
            list_type,
            start_number,
            parse_options,
          );
          idx += 1;
        }
        continue;
      }
    }

    process_mdast_node(
      document_data,
      &children[idx],
      parent_id.clone(),
      None,
      list_type,
      start_number,
      parse_options,
    );
    idx += 1;
  }
}

struct ParsedAside {
  icon: String,
  content: String,
}

fn parse_aside_html(html: &str) -> Option<ParsedAside> {
  let html = html.trim();
  if !html.starts_with("<aside>") {
    return None;
  }

  let mut content = html.trim_start_matches("<aside>").trim().to_string();
  if let Some(stripped) = content.strip_suffix("</aside>") {
    content = stripped.trim().to_string();
  }

  if content.is_empty() {
    return Some(ParsedAside {
      icon: String::new(),
      content,
    });
  }

  let mut iter = content.chars();
  let first = iter.next().unwrap_or_default();
  let mut icon = String::new();
  if !first.is_ascii_alphanumeric() {
    icon.push(first);
    content = iter.as_str().trim_start().to_string();
  }

  Some(ParsedAside { icon, content })
}

struct ParsedDetails {
  summary: String,
  body: String,
}

fn parse_details_html(html: &str) -> Option<ParsedDetails> {
  let html = html.trim();
  if !html.starts_with("<details>") {
    return None;
  }

  let mut rest = html.trim_start_matches("<details>");
  let (summary, after_summary) = extract_tag_content(rest, "summary")?;
  rest = after_summary;
  let body = rest.trim().strip_suffix("</details>").unwrap_or(rest).trim();

  Some(ParsedDetails {
    summary: summary.trim().to_string(),
    body: body.to_string(),
  })
}

fn extract_tag_content<'a>(input: &'a str, tag: &str) -> Option<(String, &'a str)> {
  let open = format!("<{}>", tag);
  let close = format!("</{}>", tag);
  let start = input.find(&open)?;
  let after_open = &input[start + open.len()..];
  let end = after_open.find(&close)?;
  let content = after_open[..end].to_string();
  let after_close = &after_open[end + close.len()..];
  Some((content, after_close))
}

fn insert_markdown_as_inline_delta(
  document_data: &mut DocumentData,
  block_id: &str,
  markdown: &str,
  parse_options: &ParseOptions,
) {
  let md = markdown.trim();
  if md.is_empty() {
    return;
  }

  let Ok(node) = to_mdast(md, parse_options) else {
    let mut delta = Delta::new();
    delta.insert(md.to_string(), Vec::new());
    insert_delta_to_text_map(document_data, block_id, delta);
    return;
  };

  if let mdast::Node::Root(root) = node {
    for child in root.children {
      match child {
        mdast::Node::Paragraph(p) => {
          for c in p.children {
            if is_inline_node(&c) {
              process_inline_mdast_node(document_data, &c, Some(block_id.to_string()));
            } else {
              let mut delta = Delta::new();
              delta.insert(c.to_string(), Vec::new());
              insert_delta_to_text_map(document_data, block_id, delta);
            }
          }
        },
        mdast::Node::Heading(h) => {
          for c in h.children {
            if is_inline_node(&c) {
              process_inline_mdast_node(document_data, &c, Some(block_id.to_string()));
            } else {
              let mut delta = Delta::new();
              delta.insert(c.to_string(), Vec::new());
              insert_delta_to_text_map(document_data, block_id, delta);
            }
          }
        },
        _ => {
          let mut delta = Delta::new();
          delta.insert(child.to_string(), Vec::new());
          insert_delta_to_text_map(document_data, block_id, delta);
        },
      }
    }
  } else {
    let mut delta = Delta::new();
    delta.insert(node.to_string(), Vec::new());
    insert_delta_to_text_map(document_data, block_id, delta);
  }
}
