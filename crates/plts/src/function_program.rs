use common::sql::quote_literal;
use pgrx::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

static ARTIFACT_SOURCE_CACHE: OnceLock<Mutex<ArtifactSourceCache>> = OnceLock::new();
static FUNCTION_PROGRAM_CACHE: OnceLock<Mutex<FunctionProgramCache>> = OnceLock::new();
const ARTIFACT_SOURCE_CACHE_CAPACITY: usize = 256;
const FUNCTION_PROGRAM_CACHE_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub(crate) struct FunctionProgram {
    pub(crate) oid: pg_sys::Oid,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) source: String,
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

    let (source, cacheable) = resolve_program_source(&row.2)?;
    let program = FunctionProgram { oid: fn_oid, schema: row.0, name: row.1, source };

    if cacheable {
        if let Ok(mut cache) = program_cache_mutex.lock() {
            cache.insert(program.clone());
        }
    }

    Some(program)
}

fn resolve_program_source(prosrc: &str) -> Option<(String, bool)> {
    if let Some(ptr) = parse_artifact_ptr(prosrc) {
        return load_compiled_artifact_from_cache_or_db(&ptr.artifact_hash)
            .map(|source| (source, false));
    }

    Some((prosrc.to_string(), true))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactPtr {
    pub(crate) artifact_hash: String,
}

#[derive(Debug, Default)]
pub(crate) struct ArtifactSourceCache {
    by_hash: HashMap<String, String>,
    lru: VecDeque<String>,
}

#[derive(Debug, Default)]
struct FunctionProgramCache {
    by_oid: HashMap<u32, FunctionProgram>,
    lru: VecDeque<u32>,
}

impl FunctionProgramCache {
    fn get(&mut self, fn_oid: pg_sys::Oid) -> Option<FunctionProgram> {
        let key = fn_oid.to_u32();
        let value = self.by_oid.get(&key)?.clone();
        self.promote(key);
        Some(value)
    }

    fn insert(&mut self, program: FunctionProgram) {
        let key = program.oid.to_u32();
        if self.by_oid.contains_key(&key) {
            self.by_oid.insert(key, program);
            self.promote(key);
            return;
        }

        if self.by_oid.len() >= FUNCTION_PROGRAM_CACHE_CAPACITY {
            while let Some(evicted) = self.lru.pop_front() {
                if self.by_oid.remove(&evicted).is_some() {
                    break;
                }
            }
        }

        self.lru.push_back(key);
        self.by_oid.insert(key, program);
    }

    fn promote(&mut self, fn_oid: u32) {
        if let Some(position) = self.lru.iter().position(|entry| *entry == fn_oid) {
            let key = self.lru.remove(position).expect("position came from lru index");
            self.lru.push_back(key);
        }
    }
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

    Some(ArtifactPtr { artifact_hash })
}

#[cfg(test)]
mod tests {
    use super::{ArtifactSourceCache, FunctionProgram, FunctionProgramCache};
    use pgrx::pg_sys;

    #[test]
    fn function_program_cache_promotes_recent_entries() {
        let mut cache = FunctionProgramCache::default();
        let first = FunctionProgram {
            oid: pg_sys::Oid::from(11_u32),
            schema: "public".to_string(),
            name: "f1".to_string(),
            source: "export default () => 1;".to_string(),
        };
        let second = FunctionProgram {
            oid: pg_sys::Oid::from(22_u32),
            schema: "public".to_string(),
            name: "f2".to_string(),
            source: "export default () => 2;".to_string(),
        };

        cache.insert(first.clone());
        cache.insert(second.clone());

        assert_eq!(cache.get(first.oid).as_ref().map(|p| p.name.as_str()), Some("f1"));
        assert_eq!(cache.get(second.oid).as_ref().map(|p| p.name.as_str()), Some("f2"));
    }

    #[test]
    fn artifact_source_cache_updates_existing_entry() {
        let mut cache = ArtifactSourceCache::default();
        cache.insert("sha256:a".to_string(), "one".to_string());
        cache.insert("sha256:a".to_string(), "two".to_string());

        assert_eq!(cache.get("sha256:a").as_deref(), Some("two"));
    }
}
