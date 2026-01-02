use collab_importer::notion::importer::ImportedInfo;
use collab_importer::notion::page::NotionPage;
use collab_importer::notion::NotionImporter;
use collab_importer::zip_tool::sync_zip::sync_unzip;
use collab_importer::zip_tool::util::remove_part_suffix;
use std::collections::HashMap;
use std::path::PathBuf;

fn normalize_name(name: &str) -> String {
  name.trim().to_ascii_lowercase()
}

fn is_32hex(s: &str) -> bool {
  if s.len() != 32 {
    return false;
  }
  s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
}

fn print_tree(pages: &[NotionPage], indent: usize, max_depth: usize) {
  if indent / 2 >= max_depth {
    return;
  }
  for page in pages {
    let id = page.notion_id.as_deref().unwrap_or("");
    let id = if is_32hex(id) { id } else { "" };
    let id_display = if id.is_empty() {
      String::new()
    } else {
      format!(" ({})", id)
    };
    println!(
      "{space}- {name} {kind}{id}",
      space = " ".repeat(indent),
      name = page.notion_name,
      kind = if page.is_dir { "[dir] " } else { "" },
      id = id_display
    );
    print_tree(&page.children, indent + 2, max_depth);
  }
}

fn collect_duplicate_ids(pages: &[NotionPage], path: &str, map: &mut HashMap<String, Vec<String>>) {
  for page in pages {
    let next_path = if path.is_empty() {
      page.notion_name.clone()
    } else {
      format!("{}/{}", path, page.notion_name)
    };

    if let Some(id) = &page.notion_id {
      if is_32hex(id) {
        map.entry(id.to_ascii_lowercase()).or_default().push(next_path.clone());
      }
    }

    collect_duplicate_ids(&page.children, &next_path, map);
  }
}

fn collect_sibling_name_collisions(pages: &[NotionPage], path: &str, out: &mut Vec<String>) {
  let mut buckets: HashMap<String, Vec<&NotionPage>> = HashMap::new();
  for page in pages {
    buckets
      .entry(normalize_name(&page.notion_name))
      .or_default()
      .push(page);
  }

  for (name, group) in buckets {
    if group.len() > 1 {
      let mut kinds = group
        .iter()
        .map(|p| {
          let id = p.notion_id.as_deref().unwrap_or("");
          let id = if is_32hex(id) { id } else { "" };
          let id_display = if id.is_empty() {
            String::new()
          } else {
            format!(":{}", id)
          };
          format!(
            "{kind}{id}",
            kind = if p.is_dir { "dir" } else { "page" },
            id = id_display
          )
        })
        .collect::<Vec<_>>();
      kinds.sort();
      kinds.dedup();
      out.push(format!(
        "Sibling name collision at '{path}': name='{name}', variants={variants}",
        path = if path.is_empty() { "<root>" } else { path },
        name = name,
        variants = kinds.join(", ")
      ));
    }
  }

  for page in pages {
    let next_path = if path.is_empty() {
      page.notion_name.clone()
    } else {
      format!("{}/{}", path, page.notion_name)
    };
    collect_sibling_name_collisions(&page.children, &next_path, out);
  }
}

fn summarize(imported: &ImportedInfo) {
  println!("Imported workspace name: {}", imported.name);
  println!("Top-level views: {}", imported.views().len());
  println!("Markdown count: {}", imported.num_of_markdown());
  println!("CSV count: {}", imported.num_of_csv());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let zip_path = std::env::args()
    .nth(1)
    .map(PathBuf::from)
    .expect("usage: verify_notion_zip <path-to-export-zip>");

  let out_dir = std::env::temp_dir().join(format!(
    "notion_import_verify_{}",
    uuid::Uuid::new_v4().to_string()
  ));
  std::fs::create_dir_all(&out_dir)?;

  let default_name = zip_path
    .file_stem()
    .and_then(|s| s.to_str())
    .map(remove_part_suffix)
    .unwrap_or_else(|| "notion_export".to_string());
  let unzip = sync_unzip(zip_path, out_dir.clone(), Some(default_name))?;
  println!("Unzipped to: {}", unzip.unzip_dir.display());

  let importer = NotionImporter::new(
    1,
    &unzip.unzip_dir,
    uuid::Uuid::new_v4().to_string(),
    "http://test.appflowy.cloud".to_string(),
  )?;

  let imported = importer.import().await?;
  summarize(&imported);

  println!("\n=== Imported view tree (depth<=6) ===");
  print_tree(imported.views(), 0, 6);

  let mut id_map: HashMap<String, Vec<String>> = HashMap::new();
  collect_duplicate_ids(imported.views(), "", &mut id_map);
  let mut id_dups = id_map
    .into_iter()
    .filter(|(_, paths)| paths.len() > 1)
    .collect::<Vec<_>>();
  id_dups.sort_by(|a, b| a.0.cmp(&b.0));

  println!("\n=== Duplicate notion_id scan (32-hex only) ===");
  if id_dups.is_empty() {
    println!("No duplicated 32-hex notion_id detected.");
  } else {
    for (id, paths) in id_dups {
      println!("DUP id={}:\n{}", id, paths.join("\n"));
      println!("---");
    }
  }

  let mut sibling_collisions = vec![];
  collect_sibling_name_collisions(imported.views(), "", &mut sibling_collisions);

  println!("\n=== Sibling name collision scan (normalized name) ===");
  if sibling_collisions.is_empty() {
    println!("No sibling name collisions detected.");
  } else {
    for line in sibling_collisions {
      println!("{}", line);
    }
  }

  Ok(())
}
