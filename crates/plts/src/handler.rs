use crate::arg_mapping::{build_args_payload, is_single_jsonb_arg_function};
use crate::function_program::load_function_program;
use crate::observability::{log_info, log_warn, record_execute_error, record_execute_start};
use crate::runtime::{
    build_runtime_context, execute_program, format_runtime_error_for_sql, runtime_available,
};
use pgrx::JsonB;
use pgrx::prelude::*;

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn plts_call_handler(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    if fcinfo.is_null() {
        return pg_sys::Datum::from(0);
    }

    let flinfo = (*fcinfo).flinfo;
    if flinfo.is_null() {
        (*fcinfo).isnull = true;
        return pg_sys::Datum::from(0);
    }

    let fn_oid = (*flinfo).fn_oid;
    let args_payload = build_args_payload(fcinfo, fn_oid);

    if runtime_available() {
        if let Some(program) = load_function_program(fn_oid) {
            record_execute_start();
            log_info(&format!(
                "plts.execute start schema={} fn={} oid={}",
                program.schema, program.name, program.oid
            ));
            let context = build_runtime_context(&program, &args_payload);
            match execute_program(&program.source, &context) {
                Ok(Some(value)) => {
                    log_info(&format!(
                        "plts.execute success schema={} fn={} oid={}",
                        program.schema, program.name, program.oid
                    ));
                    if let Some(datum) = JsonB(value).into_datum() {
                        return datum;
                    }
                }
                Ok(None) => {
                    log_info(&format!(
                        "plts.execute success-null schema={} fn={} oid={}",
                        program.schema, program.name, program.oid
                    ));
                    (*fcinfo).isnull = true;
                    return pg_sys::Datum::from(0);
                }
                Err(err) => {
                    record_execute_error();
                    log_warn(&format!(
                        "plts.execute failed schema={} fn={} oid={} err={}",
                        program.schema, program.name, program.oid, err
                    ));
                    error!("{}", format_runtime_error_for_sql(&program, &err));
                }
            }
        }
    }

    let is_jsonb_single_arg = is_single_jsonb_arg_function(fn_oid);
    if is_jsonb_single_arg && (*fcinfo).nargs == 1 {
        let arg0 = (*fcinfo).args.as_ptr();
        if !arg0.is_null() && !(*arg0).isnull {
            return (*arg0).value;
        }
    }

    if let Some(datum) = JsonB(args_payload).into_datum() {
        return datum;
    }

    (*fcinfo).isnull = true;
    pg_sys::Datum::from(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn pg_finfo_plts_call_handler() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn plts_validator(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    pg_sys::Datum::from(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn pg_finfo_plts_validator() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}
