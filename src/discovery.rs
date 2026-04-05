use std::path::{Path, PathBuf};

/// A discovered .pilot/ directory with its workflow files.
pub struct DiscoveredDir {
    /// Absolute path to the .pilot/ directory.
    pub abs_path: PathBuf,
    /// True if this is the .pilot/ in the current working directory.
    pub is_cwd: bool,
    /// Workflow names (YAML filenames without extension), sorted.
    pub workflows: Vec<String>,
}

/// Walk up from cwd, collect .pilot/ directories that contain YAML files.
/// Appends ~/.pilot/ if it wasn't already visited during the walk.
pub fn discover_workflows() -> Vec<DiscoveredDir> {
    let cwd = match std::env::current_dir() {
        Ok(p) => p,
        Err(_) => return vec![],
    };

    let home = home_dir();
    let home_pilot = home.as_ref().map(|h| h.join(".pilot"));

    let mut results = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut current = Some(cwd.as_path());
    let is_first = std::cell::Cell::new(true);

    while let Some(dir) = current {
        let pilot_dir = dir.join(".pilot");
        if pilot_dir.is_dir() {
            let abs = match pilot_dir.canonicalize() {
                Ok(p) => p,
                Err(_) => pilot_dir.clone(),
            };

            if !visited.contains(&abs) {
                let workflows = list_yaml_files(&pilot_dir);
                if !workflows.is_empty() {
                    let is_cwd = is_first.get();
                    results.push(DiscoveredDir {
                        abs_path: abs.clone(),
                        is_cwd,
                        workflows,
                    });
                    visited.insert(abs);
                }
            }
        }
        is_first.set(false);
        current = dir.parent();
    }

    // Append ~/.pilot/ if not already visited
    if let Some(ref hp) = home_pilot {
        if hp.is_dir() {
            let abs = hp.canonicalize().unwrap_or_else(|_| hp.clone());
            if !visited.contains(&abs) {
                let workflows = list_yaml_files(hp);
                if !workflows.is_empty() {
                    results.push(DiscoveredDir {
                        abs_path: abs,
                        is_cwd: false,
                        workflows,
                    });
                }
            }
        }
    }

    results
}

/// Convert an absolute path to ~/... form if it's under the home directory.
pub fn tilde_path(abs_path: &Path) -> Option<String> {
    let home = home_dir()?;
    abs_path
        .strip_prefix(&home)
        .ok()
        .map(|rel| format!("~/{}", rel.display()))
}

/// List YAML files in a directory, returning names without extension, sorted.
fn list_yaml_files(dir: &Path) -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "yaml" || ext == "yml" {
                        if let Some(stem) = path.file_stem() {
                            names.push(stem.to_string_lossy().to_string());
                        }
                    }
                }
            }
        }
    }
    names.sort();
    names
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }
}
