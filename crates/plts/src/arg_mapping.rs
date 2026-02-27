use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

static ARG_TYPE_CACHE: OnceLock<Mutex<ArgTypeCache>> = OnceLock::new();
const ARG_TYPE_CACHE_CAPACITY: usize = 512;

pub(crate) fn is_single_jsonb_arg_function(fn_oid: pg_sys::Oid) -> bool {
    let arg_oids = get_arg_type_oids(fn_oid);
    arg_oids.len() == 1 && arg_oids[0] == pg_sys::JSONBOID
}

pub(crate) unsafe fn build_args_payload(
    fcinfo: pg_sys::FunctionCallInfo,
    fn_oid: pg_sys::Oid,
) -> Value {
    let arg_oids = get_arg_type_oids(fn_oid);
    let nargs = (*fcinfo).nargs as usize;
    let mut positional = Vec::with_capacity(nargs);
    let mut named = serde_json::Map::with_capacity(nargs);

    for i in 0..nargs {
        let arg = *(*fcinfo).args.as_ptr().add(i);
        let oid = arg_oids.get(i).copied().unwrap_or(pg_sys::UNKNOWNOID);
        let value = if arg.isnull { Value::Null } else { datum_to_json_value(arg.value, oid) };

        positional.push(value.clone());
        named.insert(i.to_string(), value);
    }

    let mut payload = serde_json::Map::with_capacity(2);
    payload.insert("positional".to_string(), Value::Array(positional));
    payload.insert("named".to_string(), Value::Object(named));
    Value::Object(payload)
}

unsafe fn datum_to_json_value(datum: pg_sys::Datum, oid: pg_sys::Oid) -> Value {
    match oid {
        pg_sys::TEXTOID => {
            String::from_datum(datum, false).map(Value::String).unwrap_or(Value::Null)
        }
        pg_sys::INT4OID => i32::from_datum(datum, false)
            .map(|v| Value::Number(serde_json::Number::from(v)))
            .unwrap_or(Value::Null),
        pg_sys::BOOLOID => bool::from_datum(datum, false).map(Value::Bool).unwrap_or(Value::Null),
        pg_sys::JSONBOID => JsonB::from_datum(datum, false).map(|v| v.0).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn get_arg_type_oids(fn_oid: pg_sys::Oid) -> Vec<pg_sys::Oid> {
    let cache_mutex = ARG_TYPE_CACHE.get_or_init(|| Mutex::new(ArgTypeCache::default()));

    if let Ok(mut cache) = cache_mutex.lock() {
        if let Some(cached) = cache.get(fn_oid) {
            return cached;
        }
    }

    let sql = format!(
        "
        SELECT COALESCE(array_to_string(p.proargtypes::oid[], ','), '')
        FROM pg_proc p
        WHERE p.oid = {}
        ",
        fn_oid
    );

    let csv = Spi::get_one::<String>(&sql).ok().flatten().unwrap_or_default();
    if csv.is_empty() {
        if let Ok(mut cache) = cache_mutex.lock() {
            cache.insert(fn_oid, &[]);
        }
        return Vec::new();
    }

    let parsed: Vec<pg_sys::Oid> = csv
        .split(',')
        .filter_map(|raw| raw.trim().parse::<u32>().ok())
        .map(pg_sys::Oid::from)
        .collect();

    if let Ok(mut cache) = cache_mutex.lock() {
        cache.insert(fn_oid, &parsed);
    }

    parsed
}

#[derive(Debug, Default)]
struct ArgTypeCache {
    by_oid: HashMap<u32, Vec<pg_sys::Oid>>,
    lru: VecDeque<u32>,
}

impl ArgTypeCache {
    fn get(&mut self, fn_oid: pg_sys::Oid) -> Option<Vec<pg_sys::Oid>> {
        let key = fn_oid.to_u32();
        let value = self.by_oid.get(&key)?.clone();
        self.promote(key);
        Some(value)
    }

    fn insert(&mut self, fn_oid: pg_sys::Oid, arg_types: &[pg_sys::Oid]) {
        let key = fn_oid.to_u32();
        if self.by_oid.contains_key(&key) {
            self.by_oid.insert(key, arg_types.to_vec());
            self.promote(key);
            return;
        }

        if self.by_oid.len() >= ARG_TYPE_CACHE_CAPACITY {
            while let Some(evicted) = self.lru.pop_front() {
                if self.by_oid.remove(&evicted).is_some() {
                    break;
                }
            }
        }

        self.lru.push_back(key);
        self.by_oid.insert(key, arg_types.to_vec());
    }

    fn promote(&mut self, fn_oid: u32) {
        if let Some(position) = self.lru.iter().position(|entry| *entry == fn_oid) {
            let key = self.lru.remove(position).expect("position came from lru index");
            self.lru.push_back(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArgTypeCache;
    use pgrx::pg_sys;

    #[test]
    fn arg_type_cache_returns_inserted_values() {
        let mut cache = ArgTypeCache::default();
        let fn_oid = pg_sys::Oid::from(42_u32);
        cache.insert(fn_oid, &[pg_sys::INT4OID, pg_sys::JSONBOID]);

        let cached = cache.get(fn_oid).expect("cached entry should exist");
        assert_eq!(cached, vec![pg_sys::INT4OID, pg_sys::JSONBOID]);
    }

    #[test]
    fn arg_type_cache_overwrites_existing_values() {
        let mut cache = ArgTypeCache::default();
        let fn_oid = pg_sys::Oid::from(77_u32);
        cache.insert(fn_oid, &[pg_sys::INT4OID]);
        cache.insert(fn_oid, &[pg_sys::JSONBOID]);

        let cached = cache.get(fn_oid).expect("cached entry should exist");
        assert_eq!(cached, vec![pg_sys::JSONBOID]);
    }
}
