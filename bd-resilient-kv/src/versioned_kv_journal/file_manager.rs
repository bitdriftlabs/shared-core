use std::path::{Path, PathBuf};

/// Find the active journal file by searching for the highest generation number. If we failed
/// to read the directory or there are no journal files, we return generation 0.
pub async fn find_active_journal(dir: &Path, name: &str) -> (PathBuf, u64) {
  // Search for generation-based journals
  let pattern = format!("{name}.jrn.");

  let mut max_gen = 0u64;
  let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
    return (dir.join(format!("{name}.jrn.{max_gen}")), max_gen);
  };

  while let Ok(Some(entry)) = entries.next_entry().await {
    let filename = entry.file_name();
    let filename_str = filename.to_string_lossy();

    if let Some(suffix) = filename_str.strip_prefix(&pattern) {
      // Parse generation number (before any .zz or other extensions)
      if let Some(gen_str) = suffix.split('.').next()
        && let Ok(generation) = gen_str.parse::<u64>()
      {
        max_gen = max_gen.max(generation);
      }
    }
  }

  let path = dir.join(format!("{name}.jrn.{max_gen}"));
  (path, max_gen)
}

/// Compress an archived journal using zlib.
///
/// This function compresses the source file to the destination using zlib compression.
/// The compression is performed in a blocking task to avoid holding up the async runtime.
pub async fn compress_archived_journal(source: &Path, dest: &Path) -> anyhow::Result<()> {
  let source = source.to_owned();
  let dest = dest.to_owned();

  tokio::task::spawn_blocking(move || {
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::{BufReader, copy};

    let source_file = std::fs::File::open(&source)?;
    let dest_file = std::fs::File::create(&dest)?;
    let mut encoder = ZlibEncoder::new(dest_file, Compression::new(5));
    copy(&mut BufReader::new(source_file), &mut encoder)?;
    encoder.finish()?;
    Ok::<_, anyhow::Error>(())
  })
  .await?
}
