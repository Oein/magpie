//! Daemon configuration and on-disk layout.
//!
//! Lives under the platform config dir:
//! * Linux  : `$XDG_CONFIG_HOME/better-fdm` or `~/.config/better-fdm`
//! * macOS  : `~/Library/Application Support/better-fdm`
//!
//! `config.json` holds the port, the shared token and limits; `path-resolver.js`
//! holds the user's dynamic-path script.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const DEFAULT_PORT: u16 = 7675;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    /// Shared secret the extension / GUI / CLI must present to connect.
    pub token: String,
    pub download_dir: PathBuf,
    /// Global aggregate speed cap in bytes/sec (`None` = unlimited).
    #[serde(default)]
    pub global_speed_limit: Option<u64>,
    #[serde(default)]
    pub max_connections_per_download: Option<usize>,
}

impl Config {
    pub fn config_dir() -> PathBuf {
        #[cfg(target_os = "macos")]
        {
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join("Library/Application Support/Magpie");
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
                if !x.is_empty() {
                    return PathBuf::from(x).join("magpie");
                }
            }
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(".config/magpie");
            }
        }
        std::env::temp_dir().join("magpie")
    }

    pub fn config_path() -> PathBuf {
        Self::config_dir().join("config.json")
    }
    pub fn script_path() -> PathBuf {
        Self::config_dir().join("path-resolver.js")
    }

    fn default_download_dir() -> PathBuf {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join("Downloads");
        }
        std::env::temp_dir()
    }

    /// Load config, creating defaults (and the example script) on first run.
    pub fn load_or_init() -> anyhow::Result<Config> {
        let dir = Self::config_dir();
        std::fs::create_dir_all(&dir)?;

        let path = Self::config_path();
        let cfg = if path.exists() {
            let bytes = std::fs::read(&path)?;
            serde_json::from_slice(&bytes)?
        } else {
            let cfg = Config {
                port: DEFAULT_PORT,
                token: uuid::Uuid::new_v4().to_string(),
                download_dir: Self::default_download_dir(),
                global_speed_limit: None,
                max_connections_per_download: None,
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&cfg)?)?;
            cfg
        };

        let script = Self::script_path();
        if !script.exists() {
            std::fs::write(&script, DEFAULT_SCRIPT)?;
        }
        Ok(cfg)
    }

    pub fn save(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(Self::config_dir())?;
        std::fs::write(Self::config_path(), serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    pub fn read_script() -> String {
        std::fs::read_to_string(Self::script_path()).unwrap_or_default()
    }
}

pub const DEFAULT_SCRIPT: &str = r#"// better-fdm dynamic download-path resolver.
//
// Define `resolvePath(info)` returning a destination path (absolute, or
// starting with ~/, or relative to the default download dir). It may be async.
//
// info = { url, filename, mime, size, source_page, host, ext }
async function resolvePath(info) {
    // Example: sort by file type.
    const images = ["jpg", "jpeg", "png", "gif", "webp", "svg"];
    const videos = ["mp4", "mkv", "mov", "avi", "webm"];
    const docs   = ["pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx"];

    if (images.includes(info.ext)) return "~/Downloads/Images/" + info.filename;
    if (videos.includes(info.ext)) return "~/Downloads/Videos/" + info.filename;
    if (docs.includes(info.ext))   return "~/Downloads/Documents/" + info.filename;

    // Example: group by source site.
    // if (info.host) return "~/Downloads/" + info.host + "/" + info.filename;

    return info.filename; // default: straight into the download dir
}
"#;
