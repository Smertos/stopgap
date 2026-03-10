use pgrx::JsonB;
use pgrx::pg_catalog::pg_proc::PgProc;
use pgrx::pg_getarg_type;
use pgrx::prelude::*;
use serde_json::Value;

pub(crate) fn is_single_jsonb_arg_function(
    fcinfo: pg_sys::FunctionCallInfo,
    fn_oid: pg_sys::Oid,
) -> bool {
    let arg_oids = get_arg_type_oids(fcinfo, fn_oid);
    arg_oids.len() == 1 && arg_oids[0] == pg_sys::JSONBOID
}

pub(crate) unsafe fn build_args_payload(
    fcinfo: pg_sys::FunctionCallInfo,
    fn_oid: pg_sys::Oid,
) -> Value {
    let arg_oids = get_arg_type_oids(fcinfo, fn_oid);
    let nargs = unsafe { (*fcinfo).nargs as usize };
    let mut positional = Vec::with_capacity(nargs);
    let mut named = serde_json::Map::with_capacity(nargs);

    for i in 0..nargs {
        let arg = unsafe { *(*fcinfo).args.as_ptr().add(i) };
        let oid = arg_oids.get(i).copied().unwrap_or(pg_sys::UNKNOWNOID);
        let value =
            if arg.isnull { Value::Null } else { unsafe { datum_to_json_value(arg.value, oid) } };

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
        pg_sys::TEXTOID => unsafe {
            String::from_datum(datum, false).map(Value::String).unwrap_or(Value::Null)
        },
        pg_sys::INT4OID => unsafe { i32::from_datum(datum, false) }
            .map(|v| Value::Number(serde_json::Number::from(v)))
            .unwrap_or(Value::Null),
        pg_sys::BOOLOID => {
            unsafe { bool::from_datum(datum, false) }.map(Value::Bool).unwrap_or(Value::Null)
        }
        pg_sys::JSONBOID => {
            unsafe { JsonB::from_datum(datum, false) }.map(|v| v.0).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn get_arg_type_oids(fcinfo: pg_sys::FunctionCallInfo, fn_oid: pg_sys::Oid) -> Vec<pg_sys::Oid> {
    let nargs = unsafe { (*fcinfo).nargs.max(0) as usize };
    let inferred: Vec<pg_sys::Oid> =
        (0..nargs).map(|i| unsafe { pg_getarg_type(fcinfo, i) }).collect();

    if inferred.iter().all(|oid| *oid != pg_sys::InvalidOid) {
        return inferred;
    }

    PgProc::new(fn_oid).map(|proc| proc.proargtypes()).unwrap_or_default()
}
