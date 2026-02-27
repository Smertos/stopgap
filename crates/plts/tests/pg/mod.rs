use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::Value;
#[cfg(feature = "v8_runtime")]
use serde_json::json;

include!("arg_conversion.rs");
include!("artifact_catalog.rs");
include!("metrics.rs");
include!("runtime_performance_baseline.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_artifact_pointer.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_async.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_contract.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_db_input_forms.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_module_imports.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_nulls.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_stopgap_wrappers.rs");
#[cfg(feature = "v8_runtime")]
include!("runtime_surface_lockdown.rs");
