use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub(crate) const ARTIFACT_SOURCE_CACHE_CAPACITY: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactPtr {
    pub(crate) artifact_hash: String,
    pub(crate) export_name: String,
    pub(crate) import_map: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub(crate) struct ArtifactSourceCache {
    by_hash: HashMap<String, String>,
    lru: VecDeque<String>,
}

#[derive(Debug)]
pub(crate) struct ProgramCache<T> {
    by_key: HashMap<u32, CachedProgram<T>>,
    lru: VecDeque<u32>,
    total_source_bytes: usize,
    max_entries: usize,
    max_source_bytes: usize,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedProgram<T> {
    program: T,
    estimated_source_bytes: usize,
    expires_at: Instant,
}

impl<T> ProgramCache<T> {
    pub(crate) fn new(max_entries: usize, max_source_bytes: usize, ttl: Duration) -> Self {
        Self {
            by_key: HashMap::new(),
            lru: VecDeque::new(),
            total_source_bytes: 0,
            max_entries,
            max_source_bytes,
            ttl,
        }
    }
}

impl<T: Clone> ProgramCache<T> {
    pub(crate) fn get(&mut self, key: u32) -> Option<T> {
        let now = Instant::now();
        let cached = self.by_key.get(&key)?.clone();
        if cached.expires_at <= now {
            self.remove_key(key);
            return None;
        }

        self.promote(key);
        Some(cached.program)
    }

    pub(crate) fn insert(&mut self, key: u32, program: T, estimated_source_bytes: usize) {
        if estimated_source_bytes > self.max_source_bytes {
            self.remove_key(key);
            return;
        }

        let cached = CachedProgram {
            program,
            estimated_source_bytes,
            expires_at: Instant::now() + self.ttl,
        };

        if self.by_key.contains_key(&key) {
            if let Some(previous) = self.by_key.insert(key, cached) {
                self.total_source_bytes =
                    self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
            }
            self.total_source_bytes += estimated_source_bytes;
            self.promote(key);
            return;
        }

        while self.by_key.len() >= self.max_entries
            || self.total_source_bytes + estimated_source_bytes > self.max_source_bytes
        {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };

            if let Some(previous) = self.by_key.remove(&evicted) {
                self.total_source_bytes =
                    self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
            }
        }

        self.lru.push_back(key);
        self.total_source_bytes += estimated_source_bytes;
        self.by_key.insert(key, cached);
    }

    fn promote(&mut self, key: u32) {
        if let Some(position) = self.lru.iter().position(|entry| *entry == key) {
            let key = self.lru.remove(position).expect("position came from lru index");
            self.lru.push_back(key);
        }
    }

    fn remove_key(&mut self, key: u32) {
        if let Some(previous) = self.by_key.remove(&key) {
            self.total_source_bytes =
                self.total_source_bytes.saturating_sub(previous.estimated_source_bytes);
        }

        if let Some(position) = self.lru.iter().position(|entry| *entry == key) {
            let _ = self.lru.remove(position);
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

    let export_name = parsed
        .get("export")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_string();

    Some(ArtifactPtr { artifact_hash, export_name, import_map })
}

#[cfg(test)]
mod tests {
    use super::{
        ARTIFACT_SOURCE_CACHE_CAPACITY, ArtifactSourceCache, ProgramCache, parse_artifact_ptr,
    };
    use std::time::Duration;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Program {
        name: &'static str,
    }

    #[test]
    fn parse_artifact_ptr_extracts_metadata() {
        let ptr = parse_artifact_ptr(
            r#"{"plts":1,"kind":"artifact_ptr","artifact_hash":"sha256:abc","export":"named"}"#,
        )
        .expect("expected pointer metadata");
        assert_eq!(ptr.artifact_hash, "sha256:abc");
        assert_eq!(ptr.export_name, "named");
    }

    #[test]
    fn program_cache_promotes_recent_entries() {
        let mut cache = ProgramCache::new(8, 1_024, Duration::from_secs(30));
        cache.insert(11, Program { name: "f1" }, 16);
        cache.insert(22, Program { name: "f2" }, 16);

        assert_eq!(cache.get(11).as_ref().map(|p| p.name), Some("f1"));
        assert_eq!(cache.get(22).as_ref().map(|p| p.name), Some("f2"));
    }

    #[test]
    fn program_cache_respects_source_size_budget() {
        let mut cache = ProgramCache::new(8, 128, Duration::from_secs(30));
        cache.insert(11, Program { name: "f1" }, 32);
        cache.insert(22, Program { name: "f2" }, 32);
        cache.insert(33, Program { name: "f3" }, 96);

        assert!(cache.get(11).is_none());
        assert_eq!(cache.get(22).as_ref().map(|p| p.name), Some("f2"));
        assert_eq!(cache.get(33).as_ref().map(|p| p.name), Some("f3"));
    }

    #[test]
    fn program_cache_expires_entries_after_ttl() {
        let mut cache = ProgramCache::new(8, 1_024, Duration::from_millis(1));
        cache.insert(11, Program { name: "f1" }, 16);
        std::thread::sleep(Duration::from_millis(5));
        assert!(cache.get(11).is_none());
    }

    #[test]
    fn artifact_source_cache_evicts_least_recently_used_entry() {
        let mut cache = ArtifactSourceCache::default();
        for i in 0..ARTIFACT_SOURCE_CACHE_CAPACITY {
            cache.insert(format!("hash-{i}"), format!("src-{i}"));
        }

        assert_eq!(cache.get("hash-0").as_deref(), Some("src-0"));
        cache.insert("hash-overflow".to_string(), "src-overflow".to_string());

        assert_eq!(cache.get("hash-1"), None);
        assert_eq!(cache.get("hash-0").as_deref(), Some("src-0"));
        assert_eq!(cache.get("hash-overflow").as_deref(), Some("src-overflow"));
    }

    #[test]
    fn artifact_source_cache_updates_existing_entry() {
        let mut cache = ArtifactSourceCache::default();
        cache.insert("sha256:a".to_string(), "one".to_string());
        cache.insert("sha256:a".to_string(), "two".to_string());

        assert_eq!(cache.get("sha256:a").as_deref(), Some("two"));
    }
}
