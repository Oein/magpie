//! User-scriptable download-path resolution, powered by QuickJS (via `rquickjs`).
//!
//! The user writes a small JavaScript file that defines:
//!
//! ```js
//! // `info` has: url, filename, mime, size, source_page, host, ext
//! async function resolvePath(info) {
//!     if (info.ext === "pdf") return "~/Downloads/papers/" + info.filename;
//!     if (info.host && info.host.endsWith("github.com")) return "~/code/" + info.filename;
//!     return info.filename; // relative -> goes under the default download dir
//! }
//! ```
//!
//! The function may be `async` (await fetches to a metadata service, etc.). The
//! sandbox has **no file or network access** of its own and runs under a memory
//! limit and a wall-clock deadline.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magpie_core::PathResolver;
use magpie_proto::PathContext;
use rquickjs::{async_with, AsyncContext, AsyncRuntime, CatchResultExt, Promise};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;

const MEMORY_LIMIT: usize = 32 * 1024 * 1024; // 32 MiB ceiling for the JS heap
const DEADLINE: Duration = Duration::from_secs(5);

pub struct ScriptResolver {
    /// User script source. Empty string → behave like the default resolver.
    script: String,
    fallback_dir: PathBuf,
    /// QuickJS is single-threaded per runtime; serialize resolver calls.
    guard: Mutex<()>,
}

impl ScriptResolver {
    pub fn new(script: impl Into<String>, fallback_dir: PathBuf) -> Self {
        Self {
            script: script.into(),
            fallback_dir,
            guard: Mutex::new(()),
        }
    }

    async fn eval(&self, ctx: &PathContext) -> anyhow::Result<String> {
        let ctx_json = serde_json::to_string(ctx)?;
        let script = self.script.clone();
        // QuickJS runtimes are not `Send`, and the engine needs a `Send` future.
        // Run the whole evaluation on a blocking thread with its own
        // current-thread runtime, and hand back only the resulting `String`.
        let joined = tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(run_js(&script, &ctx_json))
        })
        .await?;
        joined
    }

    fn to_path(&self, raw: &str) -> PathBuf {
        let raw = raw.trim();
        if let Some(rest) = raw.strip_prefix("~/") {
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
        let p = PathBuf::from(raw);
        if p.is_absolute() {
            p
        } else {
            self.fallback_dir.join(p)
        }
    }
}

impl PathResolver for ScriptResolver {
    fn resolve(
        &self,
        ctx: PathContext,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PathBuf>> + Send + '_>> {
        Box::pin(async move {
            if self.script.trim().is_empty() {
                return Ok(self.fallback_dir.join(magpie_core::sanitize(&ctx.filename)));
            }
            let _lock = self.guard.lock().await;
            let raw = self.eval(&ctx).await?;
            Ok(self.to_path(&raw))
        })
    }
}

/// The `!Send` QuickJS evaluation, driven on a current-thread runtime.
async fn run_js(script: &str, ctx_json: &str) -> anyhow::Result<String> {
    let wrapper = format!(
        "(async () => {{\n\
            const info = {json};\n\
            {user}\n\
            if (typeof resolvePath !== 'function') {{ throw new Error('script must define a resolvePath(info) function'); }}\n\
            const r = await resolvePath(info);\n\
            if (typeof r !== 'string' || r.length === 0) {{ throw new Error('resolvePath must return a non-empty path string'); }}\n\
            return r;\n\
        }})()",
        json = ctx_json,
        user = script,
    );

    let rt = AsyncRuntime::new()?;
    rt.set_memory_limit(MEMORY_LIMIT).await;
    let deadline = Instant::now() + DEADLINE;
    rt.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)))
        .await;
    let context = AsyncContext::full(&rt).await?;

    let fut = async_with!(context => |ctx| {
        let promise: Promise = ctx
            .eval(wrapper.as_bytes())
            .catch(&ctx)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let path: String = promise
            .into_future::<String>()
            .await
            .catch(&ctx)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok::<String, anyhow::Error>(path)
    });

    match tokio::time::timeout(DEADLINE + Duration::from_secs(1), fut).await {
        Ok(r) => r,
        Err(_) => anyhow::bail!("path script exceeded time limit"),
    }
}

/// Convenience: wrap a resolver in an `Arc<dyn PathResolver>`.
pub fn boxed(resolver: ScriptResolver) -> Arc<dyn PathResolver> {
    Arc::new(resolver)
}
