use crate::compiler::{TsgoServiceResponse, decode_tsgo_service_response, tsgo_wasm_runtime};
use crate::observability::{
    log_info, log_warn, record_compiler_service_error, record_compiler_service_exec,
    record_compiler_service_queue_depth, record_compiler_service_queue_wait,
    record_compiler_service_reactor_init, record_compiler_service_request,
    record_compiler_service_restart, record_compiler_service_transport, record_tsgo_wasm_init_start,
    record_tsgo_wasm_init_success,
};
use crate::{
    compiler_reactor_max_age_seconds, compiler_reactor_max_requests, compiler_request_timeout_ms,
};
use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::prelude::*;
use pgrx::{PGRXSharedMemory, PgLwLock, pg_shmem_init};
use serde_json::json;
use std::ptr::{self, null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use wasmtime::{Instance, Linker, Memory, Store, TypedFunc};
use wasmtime_wasi::WasiCtxBuilder;
use wasmtime_wasi::pipe::{MemoryInputPipe, MemoryOutputPipe};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};

const COMPILER_QUEUE_CAPACITY: usize = 64;
const COMPILER_REQUEST_MQ_BYTES: usize = 128 * 1024;
const COMPILER_TOC_MAGIC: u64 = 0x504c54535f545347;
const COMPILER_TOC_REQUEST_MQ: u64 = 1;
const COMPILER_TOC_RESPONSE_MQ: u64 = 2;
const COMPILER_DSM_BYTES: usize = 16 * 1024 + (COMPILER_REQUEST_MQ_BYTES * 2);
const SERVICE_STATUS_STARTING: u8 = 0;
const SERVICE_STATUS_READY: u8 = 1;
const SERVICE_STATUS_DEGRADED: u8 = 2;
const SERVICE_STATUS_FAILED: u8 = 3;
const SLOT_STATE_FREE: u8 = 0;
const SLOT_STATE_QUEUED: u8 = 1;
const SLOT_STATE_CLAIMED: u8 = 2;
const MQ_SUCCESS: u32 = pg_sys::shm_mq_result::SHM_MQ_SUCCESS;
const MQ_WOULD_BLOCK: u32 = pg_sys::shm_mq_result::SHM_MQ_WOULD_BLOCK;
const MQ_DETACHED: u32 = pg_sys::shm_mq_result::SHM_MQ_DETACHED;
const WAIT_FLAGS: i32 =
    (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as i32;

pub(crate) static COMPILER_SERVICE_STATE: PgLwLock<CompilerServiceState> =
    unsafe { PgLwLock::new(c"plts_compiler_service") };
static COMPILER_SERVICE_PRELOADED: AtomicBool = AtomicBool::new(false);
static BACKEND_SERVICE_INIT_RECORDED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompilerRequestKind {
    Typecheck,
    Transpile,
    CompileChecked,
}

impl CompilerRequestKind {
    pub(crate) fn operation_name(self) -> &'static str {
        match self {
            Self::Typecheck => "typecheck",
            Self::Transpile => "transpile",
            Self::CompileChecked => "compile_checked",
        }
    }

    pub(crate) fn label(self) -> &'static str {
        self.operation_name()
    }

    pub(crate) fn includes_source_map(self) -> bool {
        matches!(self, Self::Transpile | Self::CompileChecked)
    }

    fn code(self) -> u8 {
        match self {
            Self::Typecheck => 1,
            Self::Transpile => 2,
            Self::CompileChecked => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Typecheck),
            2 => Some(Self::Transpile),
            3 => Some(Self::CompileChecked),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct CompilerRequestSlot {
    request_id: u64,
    request_kind: u8,
    state: u8,
    _reserved: [u8; 2],
    dsm_handle: u32,
    client_pid: i32,
}

impl Default for CompilerRequestSlot {
    fn default() -> Self {
        Self {
            request_id: 0,
            request_kind: 0,
            state: SLOT_STATE_FREE,
            _reserved: [0; 2],
            dsm_handle: 0,
            client_pid: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for CompilerRequestSlot {}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct CompilerServiceState {
    status: u8,
    _pad0: [u8; 3],
    worker_pid: i32,
    worker_proc_number: i32,
    generation: u64,
    next_request_id: u64,
    head: u32,
    tail: u32,
    depth: u32,
    slots: [CompilerRequestSlot; COMPILER_QUEUE_CAPACITY],
}

impl Default for CompilerServiceState {
    fn default() -> Self {
        Self {
            status: SERVICE_STATUS_STARTING,
            _pad0: [0; 3],
            worker_pid: 0,
            worker_proc_number: -1,
            generation: 0,
            next_request_id: 1,
            head: 0,
            tail: 0,
            depth: 0,
            slots: [CompilerRequestSlot::default(); COMPILER_QUEUE_CAPACITY],
        }
    }
}

unsafe impl PGRXSharedMemory for CompilerServiceState {}

pub(crate) fn init_compiler_service_shared_memory() {
    pg_shmem_init!(COMPILER_SERVICE_STATE = CompilerServiceState::default());
}

pub(crate) fn mark_compiler_service_preloaded() {
    COMPILER_SERVICE_PRELOADED.store(true, Ordering::Relaxed);
}

pub(crate) fn register_compiler_service_worker() {
    BackgroundWorkerBuilder::new("plts compiler worker")
        .set_library("plts")
        .set_function("plts_compiler_worker_main")
        .set_type("plts compiler worker")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_shmem_access(None)
        .load();
}

pub(crate) fn compiler_service_request(
    request_kind: CompilerRequestKind,
    request_json: Vec<u8>,
) -> Result<Vec<u8>, String> {
    if !compiler_service_preloaded() {
        record_compiler_service_error("worker_dead");
        return execute_request_locally(
            request_kind,
            &request_json,
            "plts compiler service unavailable: plts must be loaded via shared_preload_libraries and PostgreSQL must be restarted",
        );
    }

    let started_at = Instant::now();
    let (worker_pid, worker_proc_number) = match wait_for_worker_identity() {
        Ok(identity) => identity,
        Err(err) => return execute_request_locally(request_kind, &request_json, &err),
    };
    let worker_proc = lookup_proc(worker_pid, worker_proc_number);
    if worker_proc.is_null() {
        record_compiler_service_error("worker_dead");
        return execute_request_locally(
            request_kind,
            &request_json,
            "plts compiler service unavailable: compiler worker is not running",
        );
    }

    let transport =
        RequestTransport::create(worker_proc, unsafe { pg_sys::MyProc }, request_json.len())?;
    let request_id = enqueue_request(request_kind, transport.handle, unsafe { pg_sys::MyProcPid })?;
    record_compiler_service_request(request_kind);
    record_compiler_service_transport(request_json.len() as u64, 0);
    record_compiler_service_queue_wait(started_at.elapsed().as_millis() as u64);

    wake_worker(worker_pid);
    if let Err(err) = send_bytes_with_timeout(
        transport.request_handle,
        &request_json,
        compiler_request_timeout_ms(),
        "request send",
    ) {
        clear_request_slot(request_id);
        return execute_request_locally(request_kind, &request_json, &err);
    }
    let response_json = match receive_bytes_with_timeout(
        transport.response_handle,
        compiler_request_timeout_ms(),
        "response receive",
    ) {
        Ok(response_json) => response_json,
        Err(err) => {
            clear_request_slot(request_id);
            return execute_request_locally(request_kind, &request_json, &err);
        }
    };
    record_compiler_service_transport(request_json.len() as u64, response_json.len() as u64);
    record_compiler_service_queue_depth(current_queue_depth() as u64);
    complete_claim(request_id);
    record_backend_service_runtime_init();
    drop(transport);
    Ok(response_json)
}

fn record_backend_service_runtime_init() {
    if BACKEND_SERVICE_INIT_RECORDED.swap(true, Ordering::Relaxed) {
        return;
    }
    let started_at = record_tsgo_wasm_init_start();
    record_tsgo_wasm_init_success(started_at);
}

fn compiler_service_preloaded() -> bool {
    COMPILER_SERVICE_PRELOADED.load(Ordering::Relaxed)
}

fn wait_for_worker_identity() -> Result<(i32, i32), String> {
    let timeout_ms = compiler_request_timeout_ms();
    let started = Instant::now();
    loop {
        let state = COMPILER_SERVICE_STATE.share();
        let worker_pid = state.worker_pid;
        let worker_proc_number = state.worker_proc_number;
        let status = state.status;
        drop(state);

        if worker_pid > 0 {
            return Ok((worker_pid, worker_proc_number));
        }
        if status == SERVICE_STATUS_FAILED {
            return Err("plts compiler service unavailable: compiler worker failed to initialize"
                .to_string());
        }
        if started.elapsed().as_millis() >= timeout_ms as u128 {
            record_compiler_service_error("worker_dead");
            return Err("plts compiler service unavailable: compiler worker did not become ready before timeout".to_string());
        }
        wait_for_latch_slice(10);
    }
}

fn enqueue_request(
    request_kind: CompilerRequestKind,
    dsm_handle: u32,
    client_pid: i32,
) -> Result<u64, String> {
    let timeout_ms = compiler_request_timeout_ms();
    let started = Instant::now();
    loop {
        let mut state = COMPILER_SERVICE_STATE.exclusive();
        if let Some(slot_index) = find_free_slot(&state) {
            let request_id = state.next_request_id;
            state.next_request_id = state.next_request_id.saturating_add(1);
            state.depth = state.depth.saturating_add(1);
            state.tail = ((slot_index + 1) % COMPILER_QUEUE_CAPACITY) as u32;
            state.slots[slot_index] = CompilerRequestSlot {
                request_id,
                request_kind: request_kind.code(),
                state: SLOT_STATE_QUEUED,
                _reserved: [0; 2],
                dsm_handle,
                client_pid,
            };
            let depth = state.depth;
            drop(state);
            log_info(&format!(
                "plts compiler service enqueued request_id={} slot={} depth={} kind={}",
                request_id,
                slot_index,
                depth,
                request_kind.label()
            ));
            return Ok(request_id);
        }
        drop(state);

        if started.elapsed().as_millis() >= timeout_ms as u128 {
            record_compiler_service_error("queue_timeout");
            return Err("plts compiler service queue timeout".to_string());
        }
        wait_for_latch_slice(10);
    }
}

fn find_free_slot(state: &CompilerServiceState) -> Option<usize> {
    (0..COMPILER_QUEUE_CAPACITY)
        .map(|offset| ((state.tail as usize) + offset) % COMPILER_QUEUE_CAPACITY)
        .find(|index| state.slots[*index].state == SLOT_STATE_FREE)
}

fn current_queue_depth() -> u32 {
    COMPILER_SERVICE_STATE.share().depth
}

fn complete_claim(request_id: u64) {
    let mut state = COMPILER_SERVICE_STATE.exclusive();
    for slot in &mut state.slots {
        if slot.request_id == request_id && slot.state == SLOT_STATE_CLAIMED {
            *slot = CompilerRequestSlot::default();
            return;
        }
    }
}

fn clear_request_slot(request_id: u64) {
    let mut state = COMPILER_SERVICE_STATE.exclusive();
    for index in 0..state.slots.len() {
        if state.slots[index].request_id == request_id {
            if state.slots[index].state == SLOT_STATE_QUEUED {
                state.depth = state.depth.saturating_sub(1);
            }
            state.slots[index] = CompilerRequestSlot::default();
            return;
        }
    }
}

fn claim_request() -> Option<(usize, CompilerRequestSlot)> {
    let mut state = COMPILER_SERVICE_STATE.exclusive();
    let slot_index = (0..COMPILER_QUEUE_CAPACITY)
        .map(|offset| ((state.head as usize) + offset) % COMPILER_QUEUE_CAPACITY)
        .find(|index| state.slots[*index].state == SLOT_STATE_QUEUED)?;
    state.depth = state.depth.saturating_sub(1);
    state.head = ((slot_index + 1) % COMPILER_QUEUE_CAPACITY) as u32;
    state.slots[slot_index].state = SLOT_STATE_CLAIMED;
    Some((slot_index, state.slots[slot_index]))
}

fn release_slot(slot_index: usize) {
    let mut state = COMPILER_SERVICE_STATE.exclusive();
    if slot_index < state.slots.len() {
        state.slots[slot_index] = CompilerRequestSlot::default();
    }
}

fn wake_worker(worker_pid: i32) {
    unsafe {
        let state = COMPILER_SERVICE_STATE.share();
        let worker_proc = lookup_proc(worker_pid, state.worker_proc_number);
        drop(state);
        if !worker_proc.is_null() {
            pg_sys::SetLatch(&raw mut (*worker_proc).procLatch);
        }
    }
}

fn lookup_proc(pid: i32, proc_number: i32) -> *mut pg_sys::PGPROC {
    unsafe {
        if proc_number >= 0 {
            let proc_ = pg_sys::ProcNumberGetProc(proc_number);
            if !proc_.is_null() {
                return proc_;
            }
        }
        let backend_proc = pg_sys::BackendPidGetProc(pid);
        if !backend_proc.is_null() {
            return backend_proc;
        }
        pg_sys::AuxiliaryPidGetProc(pid)
    }
}

struct RequestTransport {
    seg: *mut pg_sys::dsm_segment,
    handle: u32,
    request_handle: *mut pg_sys::shm_mq_handle,
    response_handle: *mut pg_sys::shm_mq_handle,
}

impl RequestTransport {
    fn create(
        worker_proc: *mut pg_sys::PGPROC,
        client_proc: *mut pg_sys::PGPROC,
        _request_bytes: usize,
    ) -> Result<Self, String> {
        unsafe {
            let seg = pg_sys::dsm_create(COMPILER_DSM_BYTES, 0);
            if seg.is_null() {
                return Err("failed to create compiler request DSM segment".to_string());
            }

            let toc = pg_sys::shm_toc_create(
                COMPILER_TOC_MAGIC,
                pg_sys::dsm_segment_address(seg),
                COMPILER_DSM_BYTES,
            );
            if toc.is_null() {
                pg_sys::dsm_detach(seg);
                return Err("failed to create compiler request TOC".to_string());
            }

            let request_region =
                pg_sys::shm_toc_allocate(toc, COMPILER_REQUEST_MQ_BYTES).cast::<std::ffi::c_void>();
            let request_mq = pg_sys::shm_mq_create(request_region, COMPILER_REQUEST_MQ_BYTES);
            pg_sys::shm_mq_set_sender(request_mq, client_proc);
            pg_sys::shm_mq_set_receiver(request_mq, worker_proc);
            pg_sys::shm_toc_insert(toc, COMPILER_TOC_REQUEST_MQ, request_mq.cast());

            let response_region =
                pg_sys::shm_toc_allocate(toc, COMPILER_REQUEST_MQ_BYTES).cast::<std::ffi::c_void>();
            let response_mq = pg_sys::shm_mq_create(response_region, COMPILER_REQUEST_MQ_BYTES);
            pg_sys::shm_mq_set_sender(response_mq, worker_proc);
            pg_sys::shm_mq_set_receiver(response_mq, client_proc);
            pg_sys::shm_toc_insert(toc, COMPILER_TOC_RESPONSE_MQ, response_mq.cast());

            let request_handle = pg_sys::shm_mq_attach(request_mq, seg, null_mut());
            let response_handle = pg_sys::shm_mq_attach(response_mq, seg, null_mut());
            if request_handle.is_null() || response_handle.is_null() {
                if !request_handle.is_null() {
                    pg_sys::shm_mq_detach(request_handle);
                }
                if !response_handle.is_null() {
                    pg_sys::shm_mq_detach(response_handle);
                }
                pg_sys::dsm_detach(seg);
                return Err("failed to attach compiler request message queue".to_string());
            }

            Ok(Self {
                handle: pg_sys::dsm_segment_handle(seg),
                seg,
                request_handle,
                response_handle,
            })
        }
    }

    fn attach(handle: u32) -> Result<Self, String> {
        unsafe {
            let seg = pg_sys::dsm_attach(handle);
            if seg.is_null() {
                return Err(format!("failed to attach compiler request DSM segment {}", handle));
            }

            let toc = pg_sys::shm_toc_attach(COMPILER_TOC_MAGIC, pg_sys::dsm_segment_address(seg));
            if toc.is_null() {
                pg_sys::dsm_detach(seg);
                return Err("failed to attach compiler request TOC".to_string());
            }

            let request_mq = pg_sys::shm_toc_lookup(toc, COMPILER_TOC_REQUEST_MQ, false)
                .cast::<pg_sys::shm_mq>();
            let response_mq = pg_sys::shm_toc_lookup(toc, COMPILER_TOC_RESPONSE_MQ, false)
                .cast::<pg_sys::shm_mq>();
            let request_handle = pg_sys::shm_mq_attach(request_mq, seg, null_mut());
            let response_handle = pg_sys::shm_mq_attach(response_mq, seg, null_mut());
            if request_handle.is_null() || response_handle.is_null() {
                if !request_handle.is_null() {
                    pg_sys::shm_mq_detach(request_handle);
                }
                if !response_handle.is_null() {
                    pg_sys::shm_mq_detach(response_handle);
                }
                pg_sys::dsm_detach(seg);
                return Err("failed to attach worker compiler message queue".to_string());
            }

            Ok(Self { seg, handle, request_handle, response_handle })
        }
    }
}

impl Drop for RequestTransport {
    fn drop(&mut self) {
        unsafe {
            if !self.request_handle.is_null() {
                pg_sys::shm_mq_detach(self.request_handle);
            }
            if !self.response_handle.is_null() {
                pg_sys::shm_mq_detach(self.response_handle);
            }
            if !self.seg.is_null() {
                pg_sys::dsm_detach(self.seg);
            }
        }
    }
}

fn send_bytes_with_timeout(
    handle: *mut pg_sys::shm_mq_handle,
    bytes: &[u8],
    timeout_ms: i32,
    stage: &str,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        if compiler_worker_shutdown_requested() {
            return Err(format!("compiler service {stage} interrupted by worker shutdown"));
        }
        maybe_process_interrupts();
        let result = unsafe {
            pg_sys::shm_mq_send(
                handle,
                bytes.len(),
                bytes.as_ptr().cast::<std::ffi::c_void>(),
                true,
                true,
            )
        };
        match result {
            MQ_SUCCESS => return Ok(()),
            MQ_WOULD_BLOCK => {
                if started.elapsed().as_millis() >= timeout_ms as u128 {
                    record_compiler_service_error("queue_timeout");
                    return Err(format!("compiler service {stage} timed out"));
                }
                wait_for_latch_slice(10);
            }
            MQ_DETACHED => {
                record_compiler_service_error("worker_dead");
                return Err(format!("compiler service {stage} detached"));
            }
            _ => {
                record_compiler_service_error("protocol");
                return Err(format!("compiler service {stage} failed with mq result {result}"));
            }
        }
    }
}

fn receive_bytes_with_timeout(
    handle: *mut pg_sys::shm_mq_handle,
    timeout_ms: i32,
    stage: &str,
) -> Result<Vec<u8>, String> {
    let started = Instant::now();
    loop {
        if compiler_worker_shutdown_requested() {
            return Err(format!("compiler service {stage} interrupted by worker shutdown"));
        }
        maybe_process_interrupts();
        let mut nbytes = 0usize;
        let mut datap = ptr::null_mut();
        let result = unsafe { pg_sys::shm_mq_receive(handle, &mut nbytes, &mut datap, true) };
        match result {
            MQ_SUCCESS => {
                let bytes =
                    unsafe { std::slice::from_raw_parts(datap.cast::<u8>(), nbytes) }.to_vec();
                return Ok(bytes);
            }
            MQ_WOULD_BLOCK => {
                if started.elapsed().as_millis() >= timeout_ms as u128 {
                    record_compiler_service_error("queue_timeout");
                    return Err(format!("compiler service {stage} timed out"));
                }
                wait_for_latch_slice(10);
            }
            MQ_DETACHED => {
                record_compiler_service_error("worker_dead");
                return Err(format!("compiler service {stage} detached"));
            }
            _ => {
                record_compiler_service_error("protocol");
                return Err(format!("compiler service {stage} failed with mq result {result}"));
            }
        }
    }
}

fn wait_for_latch_slice(timeout_ms: i32) {
    unsafe {
        if !pg_sys::MyLatch.is_null() {
            pg_sys::WaitLatch(pg_sys::MyLatch, WAIT_FLAGS, timeout_ms.into(), 0);
            pg_sys::ResetLatch(pg_sys::MyLatch);
        }
    }
}

fn compiler_worker_shutdown_requested() -> bool {
    let worker_pid = COMPILER_SERVICE_STATE.share().worker_pid;
    worker_pid > 0 && unsafe { pg_sys::MyProcPid == worker_pid } && BackgroundWorker::sigterm_received()
}

fn maybe_process_interrupts() {
    unsafe {
        if pg_sys::InterruptPending != 0 {
            pg_sys::ProcessInterrupts();
        }
    }
}

fn service_error_response(message: &str) -> Vec<u8> {
    serde_json::to_vec(&TsgoServiceResponse {
        compiled_js: String::new(),
        diagnostics: vec![crate::compiler::TsgoDiagnostic {
            severity: "error".to_string(),
            phase: Some("compiler_service".to_string()),
            message: message.to_string(),
            line: None,
            column: None,
        }],
        backend: "typescript-go".to_string(),
    })
    .unwrap_or_else(|_| {
        json!({
            "compiled_js": "",
            "diagnostics": [{
                "severity": "error",
                "phase": "compiler_service",
                "message": "compiler service failed to encode structured error"
            }],
            "backend": "typescript-go"
        })
        .to_string()
        .into_bytes()
    })
}

fn execute_request_locally(
    request_kind: CompilerRequestKind,
    request_json: &[u8],
    reason: &str,
) -> Result<Vec<u8>, String> {
    log_info(&format!(
        "plts compiler service falling back to local execution kind={} reason={reason}",
        request_kind.label()
    ));
    let response_json = execute_local_tsgo_request(request_kind, request_json)?;
    let decoded = decode_tsgo_service_response(request_kind, &response_json)?;
    if decoded.backend != "typescript-go" {
        return Err(format!("unexpected tsgo backend `{}`", decoded.backend));
    }
    Ok(response_json)
}

fn execute_local_tsgo_request(
    request_kind: CompilerRequestKind,
    request_json: &[u8],
) -> Result<Vec<u8>, String> {
    let mut reactor = TsgoReactor::new()?;
    let response_json = reactor.execute(request_json)?;
    let decoded = decode_tsgo_service_response(request_kind, &response_json)?;
    if decoded.backend != "typescript-go" {
        return Err(format!("unexpected tsgo backend `{}`", decoded.backend));
    }
    Ok(response_json)
}

struct TsgoReactor {
    store: Store<WasiP1Ctx>,
    memory: Memory,
    malloc: TypedFunc<i32, i32>,
    free: TypedFunc<(i32, i32), ()>,
    handle_request: TypedFunc<(i32, i32), i32>,
    response_ptr: TypedFunc<(), i32>,
    response_len: TypedFunc<(), i32>,
    started_at: Instant,
    requests_since_restart: u64,
}

impl TsgoReactor {
    fn new() -> Result<Self, String> {
        let init_started = Instant::now();
        let runtime = tsgo_wasm_runtime()?;
        let stdout = MemoryOutputPipe::new(64 * 1024);
        let stderr = MemoryOutputPipe::new(64 * 1024);

        let mut linker = Linker::new(&runtime.engine);
        preview1::add_to_linker_sync(&mut linker, |ctx: &mut WasiP1Ctx| ctx)
            .map_err(|err| format!("failed to wire tsgo reactor wasi linker: {err}"))?;

        let wasi = WasiCtxBuilder::new()
            .stdin(MemoryInputPipe::new(Vec::new()))
            .stdout(stdout)
            .stderr(stderr)
            .build_p1();
        let mut store = Store::new(&runtime.engine, wasi);
        let instance = linker
            .instantiate(&mut store, &runtime.module)
            .map_err(|err| format!("failed to instantiate tsgo reactor module: {err}"))?;

        initialize_reactor_instance(&mut store, &instance)?;

        let reactor = Self {
            memory: instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| "failed to locate tsgo reactor memory export".to_string())?,
            malloc: instance
                .get_typed_func::<i32, i32>(&mut store, "stopgap_malloc")
                .map_err(|err| format!("failed to locate stopgap_malloc export: {err}"))?,
            free: instance
                .get_typed_func::<(i32, i32), ()>(&mut store, "stopgap_free")
                .map_err(|err| format!("failed to locate stopgap_free export: {err}"))?,
            handle_request: instance
                .get_typed_func::<(i32, i32), i32>(&mut store, "stopgap_handle_request")
                .map_err(|err| format!("failed to locate stopgap_handle_request export: {err}"))?,
            response_ptr: instance
                .get_typed_func::<(), i32>(&mut store, "stopgap_response_ptr")
                .map_err(|err| format!("failed to locate stopgap_response_ptr export: {err}"))?,
            response_len: instance
                .get_typed_func::<(), i32>(&mut store, "stopgap_response_len")
                .map_err(|err| format!("failed to locate stopgap_response_len export: {err}"))?,
            store,
            started_at: Instant::now(),
            requests_since_restart: 0,
        };
        record_compiler_service_reactor_init(init_started.elapsed().as_millis() as u64);
        Ok(reactor)
    }

    fn execute(&mut self, request_json: &[u8]) -> Result<Vec<u8>, String> {
        let ptr = self
            .malloc
            .call(&mut self.store, request_json.len() as i32)
            .map_err(|err| format!("failed to allocate tsgo reactor request buffer: {err}"))?;
        self.memory
            .write(&mut self.store, ptr as usize, request_json)
            .map_err(|err| format!("failed to write tsgo reactor request buffer: {err}"))?;

        let status = self
            .handle_request
            .call(&mut self.store, (ptr, request_json.len() as i32))
            .map_err(|err| format!("failed to execute tsgo reactor request: {err}"))?;
        let _ = self.free.call(&mut self.store, (ptr, request_json.len() as i32));
        if status != 0 {
            return Err(format!("tsgo reactor returned failure status {status}"));
        }

        let response_ptr = self
            .response_ptr
            .call(&mut self.store, ())
            .map_err(|err| format!("failed to read tsgo reactor response ptr: {err}"))?;
        let response_len = self
            .response_len
            .call(&mut self.store, ())
            .map_err(|err| format!("failed to read tsgo reactor response len: {err}"))?;
        if response_ptr < 0 || response_len < 0 {
            return Err("tsgo reactor returned a negative response pointer or length".to_string());
        }

        let mut response = vec![0u8; response_len as usize];
        if !response.is_empty() {
            self.memory
                .read(&mut self.store, response_ptr as usize, &mut response)
                .map_err(|err| format!("failed to read tsgo reactor response buffer: {err}"))?;
            let _ = self.free.call(&mut self.store, (response_ptr, response_len));
        }

        self.requests_since_restart = self.requests_since_restart.saturating_add(1);
        Ok(response)
    }

    fn should_restart(&self) -> Option<&'static str> {
        if self.requests_since_restart >= compiler_reactor_max_requests() {
            Some("max_requests")
        } else if self.started_at.elapsed().as_secs() >= compiler_reactor_max_age_seconds() {
            Some("max_age")
        } else {
            None
        }
    }
}

fn initialize_reactor_instance(
    store: &mut Store<WasiP1Ctx>,
    instance: &Instance,
) -> Result<(), String> {
    let initialize = instance
        .get_typed_func::<(), ()>(&mut *store, "_initialize")
        .map_err(|err| format!("failed to locate tsgo reactor _initialize export: {err}"))?;
    initialize
        .call(&mut *store, ())
        .map_err(|err| format!("failed to run tsgo reactor _initialize: {err}"))?;

    let init = instance
        .get_typed_func::<(), i32>(&mut *store, "stopgap_init")
        .map_err(|err| format!("failed to locate stopgap_init export: {err}"))?;
    let status =
        init.call(&mut *store, ()).map_err(|err| format!("failed to run stopgap_init: {err}"))?;
    if status != 0 {
        return Err(format!("stopgap_init returned failure status {status}"));
    }
    Ok(())
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn plts_compiler_worker_main(_arg: pg_sys::Datum) {
    log_info("plts compiler worker entering main");
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    set_worker_state(SERVICE_STATUS_STARTING);
    unsafe {
        let mut state = COMPILER_SERVICE_STATE.exclusive();
        state.worker_pid = pg_sys::MyProcPid;
        state.worker_proc_number = pg_sys::MyProcNumber;
    }
    log_info(&format!("plts compiler worker registered pid={}", unsafe {
        pg_sys::MyProcPid
    }));
    log_info("plts compiler worker started");

    let mut reactor: Option<TsgoReactor> = None;
    set_worker_state(SERVICE_STATUS_READY);
    log_info("plts compiler worker marked ready");

    loop {
        if BackgroundWorker::sigterm_received() {
            log_info("plts compiler worker received sigterm");
            break;
        }

        if let Some((slot_index, slot)) = claim_request() {
            log_info(&format!(
                "plts compiler worker claimed request_id={} kind={} dsm_handle={}",
                slot.request_id,
                slot.request_kind,
                slot.dsm_handle
            ));
            if let Err(err) = handle_claimed_request(slot, &mut reactor) {
                log_warn(&format!(
                    "plts compiler worker request {} failed: {err}",
                    slot.request_id
                ));
            }
            release_slot(slot_index);
            continue;
        }

        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(200))) {
            log_info("plts compiler worker exiting after postmaster shutdown signal");
            break;
        }
    }

    let mut state = COMPILER_SERVICE_STATE.exclusive();
    state.worker_pid = 0;
    state.worker_proc_number = -1;
    state.status = SERVICE_STATUS_STARTING;
    log_info("plts compiler worker exiting main");
}

fn set_worker_state(status: u8) {
    let mut state = COMPILER_SERVICE_STATE.exclusive();
    state.status = status;
}

fn handle_claimed_request(
    slot: CompilerRequestSlot,
    reactor: &mut Option<TsgoReactor>,
) -> Result<(), String> {
    let request_kind = CompilerRequestKind::from_code(slot.request_kind)
        .ok_or_else(|| format!("unknown compiler request kind {}", slot.request_kind))?;
    log_info(&format!(
        "plts compiler worker attaching transport request_id={} kind={}",
        slot.request_id,
        request_kind.label()
    ));
    let transport = RequestTransport::attach(slot.dsm_handle)?;
    log_info(&format!(
        "plts compiler worker receiving request payload request_id={}",
        slot.request_id
    ));
    let request_json = receive_bytes_with_timeout(
        transport.request_handle,
        compiler_request_timeout_ms(),
        "worker request receive",
    )?;
    log_info(&format!(
        "plts compiler worker received request payload request_id={} bytes={}",
        slot.request_id,
        request_json.len()
    ));

    let exec_started = Instant::now();
    let response_json = match execute_reactor_request(reactor, request_json, request_kind) {
        Ok(response_json) => response_json,
        Err(err) => {
            record_compiler_service_error("reactor_trap");
            record_compiler_service_restart("trap");
            *reactor = None;
            service_error_response(&err)
        }
    };
    record_compiler_service_exec(request_kind, exec_started.elapsed().as_millis() as u64);
    log_info(&format!(
        "plts compiler worker sending response request_id={} bytes={}",
        slot.request_id,
        response_json.len()
    ));
    send_bytes_with_timeout(
        transport.response_handle,
        &response_json,
        compiler_request_timeout_ms(),
        "worker response send",
    )?;
    Ok(())
}

fn execute_reactor_request(
    reactor: &mut Option<TsgoReactor>,
    request_json: Vec<u8>,
    request_kind: CompilerRequestKind,
) -> Result<Vec<u8>, String> {
    if reactor.is_none() {
        log_info("plts compiler worker initializing tsgo reactor");
        *reactor = Some(TsgoReactor::new().map_err(|err| {
            set_worker_state(SERVICE_STATUS_FAILED);
            err
        })?);
        set_worker_state(SERVICE_STATUS_READY);
        log_info("plts compiler worker tsgo reactor initialized");
    }

    let reactor_ref = reactor.as_mut().expect("reactor must be initialized");
    log_info(&format!(
        "plts compiler worker executing tsgo request kind={} bytes={}",
        request_kind.label(),
        request_json.len()
    ));
    let response_json = reactor_ref.execute(&request_json)?;
    log_info(&format!(
        "plts compiler worker tsgo request complete kind={} response_bytes={}",
        request_kind.label(),
        response_json.len()
    ));
    let decoded = decode_tsgo_service_response(request_kind, &response_json)?;
    if decoded.backend != "typescript-go" {
        record_compiler_service_error("protocol");
        record_compiler_service_restart("protocol");
        *reactor = None;
        return Err(format!("unexpected tsgo backend `{}`", decoded.backend));
    }

    if let Some(reason) = reactor_ref.should_restart() {
        record_compiler_service_restart(reason);
        log_info(&format!("plts compiler worker recycling tsgo reactor reason={reason}"));
        *reactor = None;
        let mut state = COMPILER_SERVICE_STATE.exclusive();
        state.generation = state.generation.saturating_add(1);
        state.status = SERVICE_STATUS_DEGRADED;
    }

    Ok(response_json)
}

#[cfg(test)]
mod tests {
    use super::{
        COMPILER_QUEUE_CAPACITY, CompilerRequestKind, CompilerRequestSlot, CompilerServiceState,
        SLOT_STATE_CLAIMED, SLOT_STATE_FREE, SLOT_STATE_QUEUED, claim_request, find_free_slot,
    };

    #[test]
    fn request_kind_codes_round_trip() {
        for kind in [
            CompilerRequestKind::Typecheck,
            CompilerRequestKind::Transpile,
            CompilerRequestKind::CompileChecked,
        ] {
            assert_eq!(CompilerRequestKind::from_code(kind.code()), Some(kind));
        }
        assert_eq!(CompilerRequestKind::from_code(0), None);
    }

    #[test]
    fn finds_first_free_slot_from_tail() {
        let mut state = CompilerServiceState::default();
        state.tail = 10;
        state.slots[10].state = SLOT_STATE_CLAIMED;
        state.slots[11].state = SLOT_STATE_QUEUED;
        assert_eq!(find_free_slot(&state), Some(12));
    }

    #[test]
    fn default_state_has_all_free_slots() {
        let state = CompilerServiceState::default();
        assert_eq!(state.slots.len(), COMPILER_QUEUE_CAPACITY);
        assert!(state.slots.iter().all(|slot| slot.state == SLOT_STATE_FREE));
    }
}
