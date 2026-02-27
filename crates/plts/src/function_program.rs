use common::sql::quote_literal;
use pgrx::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

static ARTIFACT_SOURCE_CACHE: OnceLock<Mutex<ArtifactSourceCache>> = OnceLock::new();
static FUNCTION_PROGRAM_CACHE: OnceLock<Mutex<FunctionProgramCache>> = OnceLock::new();
const ARTIFACT_SOURCE_CACHE_CAPACITY: usize = 256;
const FUNCTION_PROGRAM_CACHE_CAPACITY: usize = 256;
const FUNCTION_PROGRAM_CACHE_MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
const FUNCTION_PROGRAM_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(crate) struct FunctionProgram {
    pub(crate) oid: pg_sys::Oid,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) source: String,
    pub(crate) bare_specifier_map: HashMap<String, String>,
}

pub(crate) fn load_function_program(fn_oid: pg_sys::Oid) -> Option<FunctionProgram> {
    let program_cache_mutex =
        FUNCTION_PROGRAM_CACHE.get_or_init(|| Mutex::new(FunctionProgramCache::default()));

    if let Ok(mut cache) = program_cache_mutex.lock() {
        if let Some(cached) = cache.get(fn_oid) {
            return Some(cached);
        }
    }

    let sql = format!(
        "
        SELECT n.nspname::text AS fn_schema,
               p.proname::text AS fn_name,
               p.prosrc::text AS prosrc
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.oid = {}
        ",
        fn_oid
    );

    let row = Spi::connect(|client| {
        let mut rows = client.select(&sql, None, &[])?;
        if let Some(row) = rows.next() {
            let schema = row.get_by_name::<String, _>("fn_schema")?.unwrap_or_default();
            let name = row.get_by_name::<String, _>("fn_name")?.unwrap_or_default();
            let prosrc = row.get_by_name::<String, _>("prosrc")?.unwrap_or_default();
            Ok::<Option<(String, String, String)>, pgrx::spi::Error>(Some((schema, name, prosrc)))
        } else {
            Ok::<Option<(String, String, String)>, pgrx::spi::Error>(None)
        }
    })
    .ok()
    .flatten()?;

    let (source, bare_specifier_map, cacheable) = resolve_program_source(&row.2)?;
    let program =
        FunctionProgram { oid: fn_oid, schema: row.0, name: row.1, source, bare_specifier_map };

    if cacheable {
        if let Ok(mut cache) = program_cache_mutex.lock() {
            cache.insert(program.clone());
        }
    }

    Some(program)
}

fn resolve_program_source(prosrc: &str) -> Option<(String, HashMap<String, String>, bool)> {
    if let Some(ptr) = parse_artifact_ptr(prosrc) {
        return load_compiled_artifact_from_cache_or_db(&ptr.artifact_hash)
            .map(|source| (source, ptr.import_map, false));
    }

    Some((prosrc.to_string(), HashMap::new(), true))
}

fn load_compiled_artifact_from_cache_or_db(artifact_hash: &str) -> Option<String> {
    let cache_mutex =
        ARTIFACT_SOURCE_CACHE.get_or_init(|| Mutex::new(ArtifactSourceCache::default()));

    if let Ok(mut cache) = cache_mutex.lock() {
        if let Some(source) = cache.get(artifact_hash) {
            return Some(source);
        }
    }

    let sql = format!(
        "SELECT compiled_js FROM plts.artifact WHERE artifact_hash = {}",
        quote_literal(artifact_hash)
    );
    let source = Spi::get_one::<String>(&sql).ok().flatten()?;

    if let Ok(mut cache) = cache_mutex.lock() {
        cache.insert(artifact_hash.to_string(), source.clone());
    }

    Some(source)
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn load_compiled_artifact_source(artifact_hash: &str) -> Option<String> {
    if artifact_hash.is_empty() {
        return None;
    }

    load_compiled_artifact_from_cache_or_db(artifact_hash)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactPtr {
    pub(crate) artifact_hash: String,
    pub(crate) import_map: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub(crate) struct ArtifactSourceCache {
    by_hash: HashMap<String, String>,
    lru: VecDeque<String>,
}

#[derive(Debug)]
struct FunctionProgramCache {
    by_oid: HashMap<u32, CachedFunctionProgram>,
    lru: VecDeque<u32>,
    total_source_bytes: usize,
    max_entries: usize,
    max_source_bytes: usize,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedFunctionProgram {
    program: FunctionProgram,
    estimated_source_bytes: usize,
    expires_at: Instant,
}

impl Default for FunctionProgramCache {
    fn default() -> Self {
        Self {
            by_oid: HashMap::new(),
            lru: VecDeque::new(),
            total_source_bytes: 0,
            max_entries: FUNCTION_PROGRAM_CACHE_CAPACITY,
            max_source_bytes: FUNCTION_PROGRAM_CACHE_MAX_SOURCE_BYTES,
            ttl: FUNCTION_PROGRAM_CACHE_TTL,
        }
    }
}

impl FunctionProgramCache {
    #[cfg(test)]
    fn with_limits(max_entries: usize, max_source_bytes: usize, ttl: Duration) -> Self {
        Self {
            by_oid: HashMap::new(),
            lru: VecDeque::new(),
            total_source_bytes: 0,
            max_entries,
            max_source_bytes,
            ttl,
        }
    }

    fn get(&mut self, fn_oid: pg_sys::Oid) -> Option<FunctionProgram> {
        let key = fn_oid.to_u32();
        let now = Instant::now();
        let cached = self.by_oid.get(&key)?.clone();
        if cached.expires_at <= now {
            self.remove_key(key);
            return None;
        }

        self.promote(key);
        Some(cached.program)
    }

    fn insert(&mut self, program: FunctionProgram) {
        let key = program.oid.to_u32();
        let estimated_source_bytes = estimate_program_size_bytes(&program);
        if estimated_source_bytes > self.max_source_bytes {
            self.remove_key(key);
            return;
        }

        let cached = CachedFunctionProgram {
            program,
            estimated_source_bytes,
            expires_at: Instant::now() + self.ttl,
        };

        if self.by_oid.contains_key(&key) {
            if let Some(previous) = self.by_oid.insert(key, cached) {
                self.total_source_bytes =
                    self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
            }
            self.total_source_bytes += estimated_source_bytes;
            self.promote(key);
            return;
        }

        while self.by_oid.len() >= self.max_entries
            || self.total_source_bytes + estimated_source_bytes > self.max_source_bytes
        {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };

            if let Some(previous) = self.by_oid.remove(&evicted) {
                self.total_source_bytes =
                    self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
            }
        }

        self.lru.push_back(key);
        self.total_source_bytes += estimated_source_bytes;
        self.by_oid.insert(key, cached);
    }

    fn promote(&mut self, fn_oid: u32) {
        if let Some(position) = self.lru.iter().position(|entry| *entry == fn_oid) {
            let key = self.lru.remove(position).expect("position came from lru index");
            self.lru.push_back(key);
        }
    }

    fn remove_key(&mut self, fn_oid: u32) {
        if let Some(previous) = self.by_oid.remove(&fn_oid) {
            self.total_source_bytes =
                self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
        }

        if let Some(position) = self.lru.iter().position(|entry| *entry == fn_oid) {
            let _ = self.lru.remove(position);
        }
    }
}

fn estimate_program_size_bytes(program: &FunctionProgram) -> usize {
    let map_bytes = program
        .bare_specifier_map
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();
    program.schema.len() + program.name.len() + program.source.len() + map_bytes
}

impl ArtifactSourceCache {
    pub(crate) fn get(&mut self, artifact_hash: &str) -> Option<String> {
        let value = self.by_hash.get(artifact_hash)?.clone();
        self.promote(artifact_hash);
        Some(value)
    }

    pub(crate) fn insert(&mut self, artifact_hash: String, source: String) {
        if self.by_hash.contains_key(&artifact_hash) {
            self.by_hash.insert(artifact_hash.clone(), source);
            self.promote(&artifact_hash);
            return;
        }

        if self.by_hash.len() >= ARTIFACT_SOURCE_CACHE_CAPACITY {
            while let Some(evicted) = self.lru.pop_front() {
                if self.by_hash.remove(&evicted).is_some() {
                    break;
                }
            }
        }

        self.lru.push_back(artifact_hash.clone());
        self.by_hash.insert(artifact_hash, source);
    }

    fn promote(&mut self, artifact_hash: &str) {
        if let Some(position) = self.lru.iter().position(|entry| entry == artifact_hash) {
            let key = self.lru.remove(position).expect("position came from lru index");
            self.lru.push_back(key);
        }
    }
}

#[cfg(test)]
pub(crate) fn artifact_source_cache_capacity() -> usize {
    ARTIFACT_SOURCE_CACHE_CAPACITY
}

pub(crate) fn parse_artifact_ptr(prosrc: &str) -> Option<ArtifactPtr> {
    let parsed = serde_json::from_str::<Value>(prosrc).ok()?;
    let kind = parsed.get("kind")?.as_str()?;
    if kind != "artifact_ptr" {
        return None;
    }

    let artifact_hash = parsed.get("artifact_hash")?.as_str()?.to_string();
    if artifact_hash.is_empty() {
        return None;
    }

    let import_map = parsed
        .get("import_map")
        .and_then(Value::as_object)
        .map(|obj| {
            obj.iter()
                .filter_map(|(key, value)| {
                    let target = value.as_str()?.trim();
                    if key.trim().is_empty() || target.is_empty() {
                        return None;
                    }
                    Some((key.clone(), target.to_string()))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();

    Some(ArtifactPtr { artifact_hash, import_map })
}

#[cfg(test)]
mod tests {
    use super::{ArtifactSourceCache, FunctionProgram, FunctionProgramCache};
    use pgrx::pg_sys;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn function_program_cache_promotes_recent_entries() {
        let mut cache = FunctionProgramCache::default();
        let first = FunctionProgram {
            oid: pg_sys::Oid::from(11_u32),
            schema: "public".to_string(),
            name: "f1".to_string(),
            source: "export default () => 1;".to_string(),
            bare_specifier_map: HashMap::new(),
        };
        let second = FunctionProgram {
            oid: pg_sys::Oid::from(22_u32),
            schema: "public".to_string(),
            name: "f2".to_string(),
            source: "export default () => 2;".to_string(),
            bare_specifier_map: HashMap::new(),
        };

        cache.insert(first.clone());
        cache.insert(second.clone());

        assert_eq!(cache.get(first.oid).as_ref().map(|p| p.name.as_str()), Some("f1"));
        assert_eq!(cache.get(second.oid).as_ref().map(|p| p.name.as_str()), Some("f2"));
    }

    #[test]
    fn function_program_cache_respects_source_size_budget() {
        let mut cache = FunctionProgramCache::with_limits(8, 120, Duration::from_secs(30));
        let mk_program = |oid: u32, name: &str, source: &str| FunctionProgram {
            oid: pg_sys::Oid::from(oid),
            schema: "public".to_string(),
            name: name.to_string(),
            source: source.to_string(),
            bare_specifier_map: HashMap::new(),
        };

        let first = mk_program(11, "f1", "export default () => 1;");
        let second = mk_program(22, "f2", "export default () => 2;");
        let larger =
            mk_program(33, "f3", "export default () => ({ value: 3333333333333333333333333 });");

        cache.insert(first.clone());
        cache.insert(second.clone());
        cache.insert(larger.clone());

        assert!(cache.get(first.oid).is_none(), "oldest entry should be evicted by byte budget");
        assert_eq!(cache.get(second.oid).as_ref().map(|p| p.name.as_str()), Some("f2"));
        assert_eq!(cache.get(larger.oid).as_ref().map(|p| p.name.as_str()), Some("f3"));
    }

    #[test]
    fn function_program_cache_expires_entries_after_ttl() {
        let mut cache = FunctionProgramCache::with_limits(8, 1024, Duration::from_millis(1));
        let program = FunctionProgram {
            oid: pg_sys::Oid::from(11_u32),
            schema: "public".to_string(),
            name: "f1".to_string(),
            source: "export default () => 1;".to_string(),
            bare_specifier_map: HashMap::new(),
        };

        cache.insert(program.clone());
        std::thread::sleep(Duration::from_millis(5));

        assert!(cache.get(program.oid).is_none(), "cache entry should expire after TTL");
    }

    #[test]
    fn artifact_source_cache_updates_existing_entry() {
        let mut cache = ArtifactSourceCache::default();
        cache.insert("sha256:a".to_string(), "one".to_string());
        cache.insert("sha256:a".to_string(), "two".to_string());

        assert_eq!(cache.get("sha256:a").as_deref(), Some("two"));
    }
}
