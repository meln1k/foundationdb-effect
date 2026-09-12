//! Native Deno FFI bridge for `foundationdb-rs`.

use foundationdb::api::NetworkAutoStop;
use foundationdb::options::{ConflictRangeType, MutationType, StreamingMode, TransactionOption};
use foundationdb::{
    Database, FdbError, KeySelector, RangeOption, Transaction, TransactionCommitError,
};
use futures::Future;
use futures::task::{ArcWake, waker_ref};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::slice;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

const BRIDGE_NOT_RUNNING: i32 = 90_000;
const INVALID_HANDLE: i32 = 90_001;
const INVALID_UTF8: i32 = 90_002;
const INVALID_ARGUMENT: i32 = 90_003;
const INVALID_TRANSACTION_STATE: i32 = 90_004;
const BRIDGE_PANIC: i32 = 90_005;
const ASYNC_CANCELLED: i32 = 90_006;

const RESULT_PRESENT: u32 = 1;
const RESULT_MORE: u32 = 2;

static LIFECYCLE: LazyLock<Mutex<Lifecycle>> =
    LazyLock::new(|| Mutex::new(Lifecycle::Uninitialized));
static ASYNC_TASKS: LazyLock<Mutex<HashMap<u64, Arc<AsyncTask>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

type CompletionCallback = unsafe extern "C" fn(u64);
type AsyncFuture = Pin<Box<dyn Future<Output = Result<FfiResult, i32>> + Send + 'static>>;

struct AsyncTask {
    request_id: u64,
    callback: CompletionCallback,
    state: Mutex<AsyncTaskState>,
}

struct AsyncTaskState {
    future: Option<AsyncFuture>,
    polling: bool,
    cancelled: bool,
    completion: Option<FfiResult>,
    delivered: bool,
}

enum Lifecycle {
    Uninitialized,
    Running {
        runtime: Arc<Runtime>,
        owners: usize,
    },
    Failed(i32),
    Stopped,
}

struct Runtime {
    api_version: i32,
    next_handle: AtomicU64,
    network: Mutex<Option<NetworkAutoStop>>,
    databases: Mutex<HashMap<u64, Arc<Database>>>,
    transactions: Mutex<HashMap<u64, Arc<Mutex<Option<TransactionState>>>>>,
    ranges: Mutex<HashMap<u64, Arc<Mutex<RangeCursor>>>>,
}

enum TransactionState {
    Active(Transaction),
    CommitFailed(TransactionCommitError),
}

struct RangeCursor {
    transaction: u64,
    range: Option<RangeOption<'static>>,
    iteration: usize,
    snapshot: bool,
}

#[repr(C)]
pub struct FfiResult {
    code: i32,
    flags: u32,
    handle: u64,
    data: Box<[u8]>,
}

impl FfiResult {
    fn ok() -> Self {
        Self {
            code: 0,
            flags: 0,
            handle: 0,
            data: Box::new([]),
        }
    }

    fn handle(handle: u64) -> Self {
        Self {
            handle,
            ..Self::ok()
        }
    }

    fn bytes(data: Vec<u8>, flags: u32) -> Self {
        Self {
            flags,
            data: data.into_boxed_slice(),
            ..Self::ok()
        }
    }

    fn error(code: i32) -> Self {
        Self { code, ..Self::ok() }
    }
}

impl AsyncTask {
    fn new(request_id: u64, callback: CompletionCallback, future: AsyncFuture) -> Self {
        Self {
            request_id,
            callback,
            state: Mutex::new(AsyncTaskState {
                future: Some(future),
                polling: false,
                cancelled: false,
                completion: None,
                delivered: false,
            }),
        }
    }

    fn poll(self: &Arc<Self>, deliver: bool) -> Option<FfiResult> {
        let mut future = {
            let mut state = self.state.lock();
            if state.delivered {
                return None;
            }
            if deliver {
                if let Some(result) = state.completion.take() {
                    state.delivered = true;
                    return Some(result);
                }
            }
            if state.polling || state.completion.is_some() {
                return None;
            }
            let future = state.future.take()?;
            state.polling = true;
            future
        };

        let waker = waker_ref(self);
        let mut context = Context::from_waker(&waker);
        let polled = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        let mut state = self.state.lock();
        state.polling = false;
        let completed = if state.cancelled {
            Some(FfiResult::error(ASYNC_CANCELLED))
        } else {
            match polled {
                Ok(Poll::Ready(result)) => Some(result.unwrap_or_else(FfiResult::error)),
                Err(_) => Some(FfiResult::error(BRIDGE_PANIC)),
                Ok(Poll::Pending) => {
                    state.future = Some(future);
                    None
                }
            }
        };

        let mut result = None;
        let notify = if let Some(completed) = completed {
            if deliver {
                state.delivered = true;
                result = Some(completed);
                false
            } else {
                state.completion = Some(completed);
                true
            }
        } else {
            false
        };
        drop(state);

        if notify {
            self.notify();
        }
        result
    }

    fn cancel(self: &Arc<Self>) {
        let future = {
            let mut state = self.state.lock();
            if state.delivered || state.completion.is_some() || state.cancelled {
                return;
            }
            state.cancelled = true;
            if state.polling {
                return;
            }
            let future = state.future.take();
            state.completion = Some(FfiResult::error(ASYNC_CANCELLED));
            future
        };
        drop(future);
        self.notify();
    }

    fn notify(&self) {
        unsafe { (self.callback)(self.request_id) };
    }
}

impl ArcWake for AsyncTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.notify();
    }
}

fn start_async(
    request_id: u64,
    callback: Option<CompletionCallback>,
    future: AsyncFuture,
) -> Result<(), i32> {
    let callback = callback.ok_or(INVALID_ARGUMENT)?;
    let task = Arc::new(AsyncTask::new(request_id, callback, future));
    {
        let mut tasks = ASYNC_TASKS.lock();
        if tasks.contains_key(&request_id) {
            return Err(INVALID_ARGUMENT);
        }
        tasks.insert(request_id, Arc::clone(&task));
    }
    let _ = task.poll(false);
    Ok(())
}

fn cancel_all_async() {
    let tasks = ASYNC_TASKS.lock().values().cloned().collect::<Vec<_>>();
    for task in tasks {
        task.cancel();
    }
}

impl Runtime {
    fn new(api_version: i32, network: NetworkAutoStop) -> Self {
        Self {
            api_version,
            next_handle: AtomicU64::new(1),
            network: Mutex::new(Some(network)),
            databases: Mutex::new(HashMap::new()),
            transactions: Mutex::new(HashMap::new()),
            ranges: Mutex::new(HashMap::new()),
        }
    }

    fn next_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    fn transaction(&self, handle: u64) -> Result<Arc<Mutex<Option<TransactionState>>>, i32> {
        self.transactions
            .lock()
            .get(&handle)
            .cloned()
            .ok_or(INVALID_HANDLE)
    }

    fn range(&self, handle: u64) -> Result<Arc<Mutex<RangeCursor>>, i32> {
        self.ranges
            .lock()
            .get(&handle)
            .cloned()
            .ok_or(INVALID_HANDLE)
    }

    fn close_transaction(&self, handle: u64) {
        self.ranges
            .lock()
            .retain(|_, cursor| cursor.lock().transaction != handle);
        self.transactions.lock().remove(&handle);
    }

    fn shutdown(&self) {
        cancel_all_async();
        self.ranges.lock().clear();
        self.transactions.lock().clear();
        self.databases.lock().clear();
        drop(self.network.lock().take());
    }
}

fn runtime() -> Result<Arc<Runtime>, i32> {
    match &*LIFECYCLE.lock() {
        Lifecycle::Running { runtime, .. } => Ok(Arc::clone(runtime)),
        Lifecycle::Failed(code) => Err(*code),
        Lifecycle::Uninitialized | Lifecycle::Stopped => Err(BRIDGE_NOT_RUNNING),
    }
}

fn result(f: impl FnOnce() -> Result<FfiResult, i32>) -> *mut FfiResult {
    let value = match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => value,
        Ok(Err(code)) => FfiResult::error(code),
        Err(_) => FfiResult::error(BRIDGE_PANIC),
    };
    Box::into_raw(Box::new(value))
}

fn code(f: impl FnOnce() -> Result<(), i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(code)) => code,
        Err(_) => BRIDGE_PANIC,
    }
}

fn fdb_code<T>(result: Result<T, FdbError>) -> Result<T, i32> {
    result.map_err(|error| error.code())
}

unsafe fn input_bytes<'a>(pointer: *const u8, length: usize) -> Result<&'a [u8], i32> {
    if length > i32::MAX as usize {
        return Err(INVALID_ARGUMENT);
    }
    if length == 0 {
        return Ok(&[]);
    }
    if pointer.is_null() {
        return Err(INVALID_ARGUMENT);
    }
    Ok(unsafe { slice::from_raw_parts(pointer, length) })
}

unsafe fn input_string<'a>(pointer: *const u8, length: usize) -> Result<&'a str, i32> {
    let bytes = unsafe { input_bytes(pointer, length) }?;
    let value = str::from_utf8(bytes).map_err(|_| INVALID_UTF8)?;
    if value.as_bytes().contains(&0) {
        return Err(INVALID_ARGUMENT);
    }
    Ok(value)
}

fn decode_keys(bytes: &[u8]) -> Result<Vec<&[u8]>, i32> {
    if bytes.len() < 4 {
        return Err(INVALID_ARGUMENT);
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| INVALID_ARGUMENT)?) as usize;
    if count > bytes.len().saturating_sub(4) / 4 {
        return Err(INVALID_ARGUMENT);
    }

    let mut keys = Vec::with_capacity(count);
    let mut offset: usize = 4;
    for _ in 0..count {
        let length_end = offset.checked_add(4).ok_or(INVALID_ARGUMENT)?;
        let length = u32::from_le_bytes(
            bytes
                .get(offset..length_end)
                .ok_or(INVALID_ARGUMENT)?
                .try_into()
                .map_err(|_| INVALID_ARGUMENT)?,
        ) as usize;
        offset = length_end;
        let key_end = offset.checked_add(length).ok_or(INVALID_ARGUMENT)?;
        keys.push(bytes.get(offset..key_end).ok_or(INVALID_ARGUMENT)?);
        offset = key_end;
    }
    if offset != bytes.len() {
        return Err(INVALID_ARGUMENT);
    }
    Ok(keys)
}

fn active_transaction(state: &Option<TransactionState>) -> Result<&Transaction, i32> {
    match state {
        Some(TransactionState::Active(transaction)) => Ok(transaction),
        Some(TransactionState::CommitFailed(_)) | None => Err(INVALID_TRANSACTION_STATE),
    }
}

fn streaming_mode(value: i32) -> Result<StreamingMode, i32> {
    match value {
        -2 => Ok(StreamingMode::WantAll),
        -1 => Ok(StreamingMode::Iterator),
        0 => Ok(StreamingMode::Exact),
        1 => Ok(StreamingMode::Small),
        2 => Ok(StreamingMode::Medium),
        3 => Ok(StreamingMode::Large),
        4 => Ok(StreamingMode::Serial),
        _ => Err(INVALID_ARGUMENT),
    }
}

fn encode_key_values(values: &foundationdb::future::FdbValues) -> Result<Vec<u8>, i32> {
    let count = u32::try_from(values.len()).map_err(|_| INVALID_ARGUMENT)?;
    let mut output = Vec::new();
    output.extend_from_slice(&count.to_le_bytes());
    for value in values.as_ref() {
        let key = value.key();
        let data = value.value();
        let key_length = u32::try_from(key.len()).map_err(|_| INVALID_ARGUMENT)?;
        let data_length = u32::try_from(data.len()).map_err(|_| INVALID_ARGUMENT)?;
        output.extend_from_slice(&key_length.to_le_bytes());
        output.extend_from_slice(&data_length.to_le_bytes());
        output.extend_from_slice(key);
        output.extend_from_slice(data);
    }
    Ok(output)
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_init(api_version: i32) -> i32 {
    code(|| {
        let mut lifecycle = LIFECYCLE.lock();
        match &mut *lifecycle {
            Lifecycle::Running { runtime, owners } if runtime.api_version == api_version => {
                *owners = owners.checked_add(1).ok_or(INVALID_ARGUMENT)?;
                return Ok(());
            }
            Lifecycle::Running { .. } | Lifecycle::Stopped => return Err(BRIDGE_NOT_RUNNING),
            Lifecycle::Failed(code) => return Err(*code),
            Lifecycle::Uninitialized => {}
        }

        let network = match foundationdb::api::FdbApiBuilder::default()
            .set_runtime_version(api_version)
            .build()
            .and_then(|builder| unsafe { builder.boot() })
        {
            Ok(network) => network,
            Err(error) => {
                let error_code = error.code();
                *lifecycle = Lifecycle::Failed(error_code);
                return Err(error_code);
            }
        };
        *lifecycle = Lifecycle::Running {
            runtime: Arc::new(Runtime::new(api_version, network)),
            owners: 1,
        };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_shutdown() -> i32 {
    code(|| {
        let runtime = {
            let mut lifecycle = LIFECYCLE.lock();
            if let Lifecycle::Running { owners, .. } = &mut *lifecycle {
                if *owners > 1 {
                    *owners -= 1;
                    return Ok(());
                }
            }
            match std::mem::replace(&mut *lifecycle, Lifecycle::Stopped) {
                Lifecycle::Running { runtime, .. } => Some(runtime),
                Lifecycle::Uninitialized | Lifecycle::Stopped => None,
                Lifecycle::Failed(error) => return Err(error),
            }
        };
        if let Some(runtime) = runtime {
            runtime.shutdown();
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `path_pointer` must reference `path_length` readable bytes when `has_path` is non-zero.
pub unsafe extern "C" fn fdb_rs_database_open(
    path_pointer: *const u8,
    path_length: usize,
    has_path: u8,
) -> *mut FfiResult {
    result(|| {
        let runtime = runtime()?;
        let path = if has_path == 0 {
            None
        } else {
            Some(unsafe { input_string(path_pointer, path_length) }?)
        };
        let database = Arc::new(fdb_code(Database::new(path))?);
        let handle = runtime.next_handle();
        runtime.databases.lock().insert(handle, database);
        Ok(FfiResult::handle(handle))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_database_close(handle: u64) -> i32 {
    code(|| {
        let runtime = runtime()?;
        runtime.databases.lock().remove(&handle);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_transaction_open(database_handle: u64) -> *mut FfiResult {
    result(|| {
        let runtime = runtime()?;
        let database = runtime
            .databases
            .lock()
            .get(&database_handle)
            .cloned()
            .ok_or(INVALID_HANDLE)?;
        let transaction = fdb_code(database.create_trx())?;
        let handle = runtime.next_handle();
        runtime.transactions.lock().insert(
            handle,
            Arc::new(Mutex::new(Some(TransactionState::Active(transaction)))),
        );
        Ok(FfiResult::handle(handle))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_transaction_close(handle: u64) -> i32 {
    code(|| {
        runtime()?.close_transaction(handle);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_transaction_set_option(handle: u64, option: u8, value: i32) -> i32 {
    code(|| {
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        let option = match option {
            0 => TransactionOption::Timeout(value),
            1 => TransactionOption::RetryLimit(value),
            2 => TransactionOption::MaxRetryDelay(value),
            _ => return Err(INVALID_ARGUMENT),
        };
        fdb_code(transaction.set_option(option))
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `key_pointer` must reference `key_length` readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_get(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
    snapshot: u8,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        let future = transaction.get(key, snapshot != 0);
        drop(state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                match fdb_code(future.await)? {
                    Some(value) => Ok(FfiResult::bytes(value.to_vec(), RESULT_PRESENT)),
                    None => Ok(FfiResult::ok()),
                }
            }),
        )
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `keys_pointer` must reference `keys_length` readable bytes containing the
/// length-prefixed key batch format accepted by this bridge.
pub unsafe extern "C" fn fdb_rs_transaction_get_many(
    handle: u64,
    keys_pointer: *const u8,
    keys_length: usize,
    snapshot: u8,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let encoded_keys = unsafe { input_bytes(keys_pointer, keys_length) }?;
        let keys = decode_keys(encoded_keys)?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        let futures = keys
            .into_iter()
            .map(|key| transaction.get(key, snapshot != 0))
            .collect::<Vec<_>>();
        drop(state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                // All reads have already been submitted to FoundationDB. Awaiting them in
                // order avoids registering thousands of Rust callbacks at once; after the
                // first wait, most local futures are already ready and poll synchronously.
                let mut values = Vec::with_capacity(futures.len());
                for future in futures {
                    values.push(fdb_code(future.await)?);
                }
                let count = u32::try_from(values.len()).map_err(|_| INVALID_ARGUMENT)?;
                let mut data = Vec::new();
                data.extend_from_slice(&count.to_le_bytes());
                for value in values {
                    if let Some(value) = value {
                        let length = u32::try_from(value.len()).map_err(|_| INVALID_ARGUMENT)?;
                        data.extend_from_slice(&length.to_le_bytes());
                        data.extend_from_slice(&value);
                    } else {
                        data.extend_from_slice(&u32::MAX.to_le_bytes());
                    }
                }
                Ok(FfiResult::bytes(data, 0))
            }),
        )
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `key_pointer` must reference `key_length` readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_get_key(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
    or_equal: u8,
    offset: i32,
    snapshot: u8,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?.to_vec();
        let selector = KeySelector::new(key.into(), or_equal != 0, offset);
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        let future = transaction.get_key(&selector, snapshot != 0);
        drop(state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                let value = fdb_code(future.await)?;
                Ok(FfiResult::bytes(value.to_vec(), RESULT_PRESENT))
            }),
        )
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_set(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
    value_pointer: *const u8,
    value_length: usize,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?;
        let value = unsafe { input_bytes(value_pointer, value_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        active_transaction(&state)?.set(key, value);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_atomic_add(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
    value_pointer: *const u8,
    value_length: usize,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?;
        let value = unsafe { input_bytes(value_pointer, value_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        active_transaction(&state)?.atomic_op(key, value, MutationType::Add);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_set_without_write_conflict(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
    value_pointer: *const u8,
    value_length: usize,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?;
        let value = unsafe { input_bytes(value_pointer, value_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        fdb_code(transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange))?;
        transaction.set(key, value);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `key_pointer` must reference `key_length` readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_clear(
    handle: u64,
    key_pointer: *const u8,
    key_length: usize,
) -> i32 {
    code(|| {
        let key = unsafe { input_bytes(key_pointer, key_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        active_transaction(&state)?.clear(key);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_clear_range(
    handle: u64,
    begin_pointer: *const u8,
    begin_length: usize,
    end_pointer: *const u8,
    end_length: usize,
) -> i32 {
    code(|| {
        let begin = unsafe { input_bytes(begin_pointer, begin_length) }?;
        let end = unsafe { input_bytes(end_pointer, end_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        active_transaction(&state)?.clear_range(begin, end);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_clear_range_without_write_conflict(
    handle: u64,
    begin_pointer: *const u8,
    begin_length: usize,
    end_pointer: *const u8,
    end_length: usize,
) -> i32 {
    code(|| {
        let begin = unsafe { input_bytes(begin_pointer, begin_length) }?;
        let end = unsafe { input_bytes(end_pointer, end_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        fdb_code(transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange))?;
        transaction.clear_range(begin, end);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_transaction_add_write_conflict_range(
    handle: u64,
    begin_pointer: *const u8,
    begin_length: usize,
    end_pointer: *const u8,
    end_length: usize,
) -> i32 {
    code(|| {
        let begin = unsafe { input_bytes(begin_pointer, begin_length) }?;
        let end = unsafe { input_bytes(end_pointer, end_length) }?;
        let runtime = runtime()?;
        let transaction = runtime.transaction(handle)?;
        let state = transaction.lock();
        fdb_code(active_transaction(&state)?.add_conflict_range(
            begin,
            end,
            ConflictRangeType::Write,
        ))
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// Both pointers must reference the corresponding number of readable bytes.
pub unsafe extern "C" fn fdb_rs_range_open(
    transaction_handle: u64,
    begin_pointer: *const u8,
    begin_length: usize,
    begin_or_equal: u8,
    begin_offset: i32,
    end_pointer: *const u8,
    end_length: usize,
    end_or_equal: u8,
    end_offset: i32,
    limit: i32,
    target_bytes: i32,
    mode: i32,
    reverse: u8,
    snapshot: u8,
) -> *mut FfiResult {
    result(|| {
        if limit < 0 || target_bytes < 0 || (mode == 0 && limit == 0) {
            return Err(INVALID_ARGUMENT);
        }
        let runtime = runtime()?;
        runtime.transaction(transaction_handle)?;
        let begin = unsafe { input_bytes(begin_pointer, begin_length) }?.to_vec();
        let end = unsafe { input_bytes(end_pointer, end_length) }?.to_vec();
        let begin = KeySelector::new(begin.into(), begin_or_equal != 0, begin_offset);
        let end = KeySelector::new(end.into(), end_or_equal != 0, end_offset);
        let mut range = RangeOption::from((begin, end));
        range.limit = (limit > 0).then_some(limit as usize);
        range.target_bytes = target_bytes as usize;
        range.mode = streaming_mode(mode)?;
        range.reverse = reverse != 0;

        let handle = runtime.next_handle();
        runtime.ranges.lock().insert(
            handle,
            Arc::new(Mutex::new(RangeCursor {
                transaction: transaction_handle,
                range: Some(range),
                iteration: 1,
                snapshot: snapshot != 0,
            })),
        );
        Ok(FfiResult::handle(handle))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_range_next(
    handle: u64,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let runtime = runtime()?;
        let cursor_state = runtime.range(handle)?;
        let mut cursor = cursor_state.lock();
        let Some(range) = cursor.range.take() else {
            return start_async(
                request_id,
                callback,
                Box::pin(async { Ok(FfiResult::bytes(Vec::new(), 0)) }),
            );
        };
        let transaction = runtime.transaction(cursor.transaction)?;
        let state = transaction.lock();
        let transaction = active_transaction(&state)?;
        let future = transaction.get_range(&range, cursor.iteration, cursor.snapshot);
        drop(state);
        drop(cursor);
        let task_cursor = Arc::clone(&cursor_state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                let values = fdb_code(future.await)?;
                let data = encode_key_values(&values)?;
                let mut cursor = task_cursor.lock();
                cursor.iteration += 1;
                cursor.range = range.next_range(&values);
                let flags = if cursor.range.is_some() {
                    RESULT_MORE
                } else {
                    0
                };
                Ok(FfiResult::bytes(data, flags))
            }),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_range_close(handle: u64) -> i32 {
    code(|| {
        runtime()?.ranges.lock().remove(&handle);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_transaction_commit(
    handle: u64,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let runtime = runtime()?;
        let transaction_state = runtime.transaction(handle)?;
        let mut state = transaction_state.lock();
        let Some(TransactionState::Active(transaction)) = state.take() else {
            return Err(INVALID_TRANSACTION_STATE);
        };
        let future = transaction.commit();
        drop(state);
        let task_state = Arc::clone(&transaction_state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                match future.await {
                    Ok(_) => Ok(FfiResult::ok()),
                    Err(error) => {
                        let error_code = error.code();
                        *task_state.lock() = Some(TransactionState::CommitFailed(error));
                        Err(error_code)
                    }
                }
            }),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_transaction_on_error(
    handle: u64,
    error_code: i32,
    callback: Option<CompletionCallback>,
    request_id: u64,
) -> i32 {
    code(|| {
        let runtime = runtime()?;
        let transaction_state = runtime.transaction(handle)?;
        let mut state = transaction_state.lock();
        let Some(transaction) = state.take() else {
            return Err(INVALID_TRANSACTION_STATE);
        };
        drop(state);
        let task_state = Arc::clone(&transaction_state);
        start_async(
            request_id,
            callback,
            Box::pin(async move {
                let result = match transaction {
                    TransactionState::Active(transaction) => {
                        transaction.on_error(FdbError::from_code(error_code)).await
                    }
                    TransactionState::CommitFailed(error) => error.on_error().await,
                };
                match result {
                    Ok(transaction) => {
                        *task_state.lock() = Some(TransactionState::Active(transaction));
                        Ok(FfiResult::ok())
                    }
                    Err(error) => Err(error.code()),
                }
            }),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_async_cancel(request_id: u64) -> i32 {
    code(|| {
        if let Some(task) = ASYNC_TASKS.lock().get(&request_id).cloned() {
            task.cancel();
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_async_poll(request_id: u64) -> *mut FfiResult {
    let task = ASYNC_TASKS.lock().get(&request_id).cloned();
    task.and_then(|task| task.poll(true))
        .map_or(std::ptr::null_mut(), |result| {
            Box::into_raw(Box::new(result))
        })
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_async_ack(request_id: u64) -> i32 {
    code(|| {
        ASYNC_TASKS.lock().remove(&request_id);
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a live pointer returned by this bridge.
pub unsafe extern "C" fn fdb_rs_result_code(result: *const FfiResult) -> i32 {
    if result.is_null() {
        INVALID_ARGUMENT
    } else {
        unsafe { (*result).code }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a live pointer returned by this bridge.
pub unsafe extern "C" fn fdb_rs_result_flags(result: *const FfiResult) -> u32 {
    if result.is_null() {
        0
    } else {
        unsafe { (*result).flags }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a live pointer returned by this bridge.
pub unsafe extern "C" fn fdb_rs_result_handle(result: *const FfiResult) -> u64 {
    if result.is_null() {
        0
    } else {
        unsafe { (*result).handle }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a live pointer returned by this bridge.
#[allow(clippy::needless_borrow)]
pub unsafe extern "C" fn fdb_rs_result_data(result: *const FfiResult) -> *const u8 {
    if result.is_null() || unsafe { (&(*result).data).is_empty() } {
        std::ptr::null()
    } else {
        unsafe { (*result).data.as_ptr() }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a live pointer returned by this bridge.
#[allow(clippy::needless_borrow)]
pub unsafe extern "C" fn fdb_rs_result_length(result: *const FfiResult) -> usize {
    if result.is_null() {
        0
    } else {
        unsafe { (&(*result).data).len() }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `result` must be null or a pointer returned by this bridge that has not yet been freed.
pub unsafe extern "C" fn fdb_rs_result_free(result: *mut FfiResult) {
    if !result.is_null() {
        drop(unsafe { Box::from_raw(result) });
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_error_message(error_code: i32) -> *mut c_char {
    let message = match error_code {
        BRIDGE_NOT_RUNNING => "FoundationDB bridge is not running",
        INVALID_HANDLE => "invalid native handle",
        INVALID_UTF8 => "input is not valid UTF-8",
        INVALID_ARGUMENT => "invalid argument",
        INVALID_TRANSACTION_STATE => "invalid transaction state",
        BRIDGE_PANIC => "native bridge panicked",
        ASYNC_CANCELLED => "native async operation cancelled",
        code => FdbError::from_code(code).message(),
    };
    CString::new(message)
        .unwrap_or_else(|_| CString::new("unknown error").unwrap())
        .into_raw()
}

#[unsafe(no_mangle)]
/// # Safety
/// `value` must be null or a pointer returned by `fdb_rs_error_message`.
pub unsafe extern "C" fn fdb_rs_string_free(value: *mut c_char) {
    if !value.is_null() {
        drop(unsafe { CString::from_raw(value) });
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_error_is_retryable(error_code: i32) -> u8 {
    (error_code < BRIDGE_NOT_RUNNING && FdbError::from_code(error_code).is_retryable()) as u8
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_error_is_maybe_committed(error_code: i32) -> u8 {
    (error_code < BRIDGE_NOT_RUNNING && FdbError::from_code(error_code).is_maybe_committed()) as u8
}

#[unsafe(no_mangle)]
pub extern "C" fn fdb_rs_error_is_retryable_not_committed(error_code: i32) -> u8 {
    (error_code < BRIDGE_NOT_RUNNING
        && FdbError::from_code(error_code).is_retryable_not_committed()) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    static ASYNC_TEST_LOCK: Mutex<()> = Mutex::new(());
    static TEST_NOTIFICATIONS: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn test_completion(_request_id: u64) {
        TEST_NOTIFICATIONS.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn take_notifications() -> usize {
        TEST_NOTIFICATIONS.swap(0, AtomicOrdering::Relaxed)
    }

    struct SelfWakingFuture {
        pending: bool,
    }

    impl Future for SelfWakingFuture {
        type Output = Result<FfiResult, i32>;

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.pending {
                self.pending = false;
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(FfiResult::bytes(vec![42], RESULT_PRESENT)))
            }
        }
    }

    #[test]
    fn input_bytes_accepts_empty_null_and_rejects_non_empty_null() {
        assert_eq!(unsafe { input_bytes(ptr::null(), 0) }, Ok(&[][..]));
        assert_eq!(
            unsafe { input_bytes(ptr::null(), 1) },
            Err(INVALID_ARGUMENT)
        );
        assert_eq!(
            unsafe { input_bytes(ptr::null(), i32::MAX as usize + 1) },
            Err(INVALID_ARGUMENT)
        );
    }

    #[test]
    fn input_string_validates_utf8_and_embedded_nul() {
        let valid = b"cluster-file";
        assert_eq!(
            unsafe { input_string(valid.as_ptr(), valid.len()) },
            Ok("cluster-file")
        );

        let invalid_utf8 = [0xff];
        assert_eq!(
            unsafe { input_string(invalid_utf8.as_ptr(), invalid_utf8.len()) },
            Err(INVALID_UTF8)
        );

        let nul = b"bad\0path";
        assert_eq!(
            unsafe { input_string(nul.as_ptr(), nul.len()) },
            Err(INVALID_ARGUMENT)
        );
    }

    #[test]
    fn key_batches_preserve_binary_and_empty_keys_and_reject_malformed_input() {
        let encoded = [
            3, 0, 0, 0, // count
            2, 0, 0, 0, 0, 255, // binary key
            0, 0, 0, 0, // empty key
            1, 0, 0, 0, 42, // final key
        ];
        assert_eq!(
            decode_keys(&encoded),
            Ok(vec![&encoded[8..10], &encoded[14..14], &encoded[18..19]])
        );

        assert_eq!(decode_keys(&[]), Err(INVALID_ARGUMENT));
        assert_eq!(decode_keys(&[1, 0, 0, 0]), Err(INVALID_ARGUMENT));
        assert_eq!(
            decode_keys(&[1, 0, 0, 0, 2, 0, 0, 0, 42]),
            Err(INVALID_ARGUMENT)
        );
        assert_eq!(decode_keys(&[0, 0, 0, 0, 42]), Err(INVALID_ARGUMENT));
    }

    #[test]
    fn streaming_modes_cover_the_public_integer_contract() {
        assert!(matches!(streaming_mode(-2), Ok(StreamingMode::WantAll)));
        assert!(matches!(streaming_mode(-1), Ok(StreamingMode::Iterator)));
        assert!(matches!(streaming_mode(0), Ok(StreamingMode::Exact)));
        assert!(matches!(streaming_mode(1), Ok(StreamingMode::Small)));
        assert!(matches!(streaming_mode(2), Ok(StreamingMode::Medium)));
        assert!(matches!(streaming_mode(3), Ok(StreamingMode::Large)));
        assert!(matches!(streaming_mode(4), Ok(StreamingMode::Serial)));
        assert!(matches!(streaming_mode(5), Err(INVALID_ARGUMENT)));
    }

    #[test]
    fn ffi_result_accessors_preserve_binary_data_and_are_null_safe() {
        let result = Box::into_raw(Box::new(FfiResult::bytes(
            vec![0, 1, 255],
            RESULT_PRESENT | RESULT_MORE,
        )));

        assert_eq!(unsafe { fdb_rs_result_code(result) }, 0);
        assert_eq!(
            unsafe { fdb_rs_result_flags(result) },
            RESULT_PRESENT | RESULT_MORE
        );
        assert_eq!(unsafe { fdb_rs_result_length(result) }, 3);
        assert_eq!(
            unsafe {
                slice::from_raw_parts(fdb_rs_result_data(result), fdb_rs_result_length(result))
            },
            [0, 1, 255]
        );
        unsafe { fdb_rs_result_free(result) };

        assert_eq!(unsafe { fdb_rs_result_code(ptr::null()) }, INVALID_ARGUMENT);
        assert_eq!(unsafe { fdb_rs_result_flags(ptr::null()) }, 0);
        assert_eq!(unsafe { fdb_rs_result_handle(ptr::null()) }, 0);
        assert_eq!(unsafe { fdb_rs_result_length(ptr::null()) }, 0);
        assert!(unsafe { fdb_rs_result_data(ptr::null()) }.is_null());
        unsafe { fdb_rs_result_free(ptr::null_mut()) };
    }

    #[test]
    fn bridge_error_messages_cross_the_ffi_boundary() {
        let pointer = fdb_rs_error_message(INVALID_HANDLE);
        assert!(!pointer.is_null());
        assert_eq!(
            unsafe { CStr::from_ptr(pointer) }.to_str(),
            Ok("invalid native handle")
        );
        unsafe { fdb_rs_string_free(pointer) };
    }

    #[test]
    fn ffi_guards_convert_errors_and_panics() {
        let error = result(|| Err(INVALID_ARGUMENT));
        assert_eq!(unsafe { fdb_rs_result_code(error) }, INVALID_ARGUMENT);
        unsafe { fdb_rs_result_free(error) };

        let panic = result(|| panic!("test panic"));
        assert_eq!(unsafe { fdb_rs_result_code(panic) }, BRIDGE_PANIC);
        unsafe { fdb_rs_result_free(panic) };

        assert_eq!(code(|| Err(INVALID_HANDLE)), INVALID_HANDLE);
        assert_eq!(code(|| panic!("test panic")), BRIDGE_PANIC);
    }

    #[test]
    fn async_task_wake_only_notifies_until_the_event_loop_polls() {
        let _guard = ASYNC_TEST_LOCK.lock();
        let request_id = 100_001;
        start_async(
            request_id,
            Some(test_completion),
            Box::pin(SelfWakingFuture { pending: true }),
        )
        .expect("task should start");

        assert_eq!(
            take_notifications(),
            1,
            "the wake should notify Deno without polling again on the waking thread"
        );
        let result = fdb_rs_async_poll(request_id);
        assert!(!result.is_null());
        assert_eq!(unsafe { fdb_rs_result_code(result) }, 0);
        assert_eq!(unsafe { fdb_rs_result_length(result) }, 1);
        assert_eq!(
            unsafe { *fdb_rs_result_data(result) },
            42,
            "callback result should preserve task output"
        );
        unsafe { fdb_rs_result_free(result) };
        assert!(fdb_rs_async_poll(request_id).is_null());
        assert_eq!(take_notifications(), 0);
        assert!(ASYNC_TASKS.lock().contains_key(&request_id));
        assert_eq!(fdb_rs_async_ack(request_id), 0);
        assert!(!ASYNC_TASKS.lock().contains_key(&request_id));
    }

    #[test]
    fn async_task_cancellation_completes_and_waits_for_acknowledgement() {
        let _guard = ASYNC_TEST_LOCK.lock();
        let request_id = 100_002;
        start_async(
            request_id,
            Some(test_completion),
            Box::pin(std::future::pending()),
        )
        .expect("task should start");

        assert_eq!(fdb_rs_async_cancel(request_id), 0);
        assert_eq!(take_notifications(), 1);
        let result = fdb_rs_async_poll(request_id);
        assert!(!result.is_null());
        assert_eq!(unsafe { fdb_rs_result_code(result) }, ASYNC_CANCELLED);
        unsafe { fdb_rs_result_free(result) };
        assert!(ASYNC_TASKS.lock().contains_key(&request_id));
        assert_eq!(fdb_rs_async_ack(request_id), 0);
        assert!(!ASYNC_TASKS.lock().contains_key(&request_id));
    }

    #[test]
    fn async_task_converts_poll_panics_to_bridge_errors() {
        let _guard = ASYNC_TEST_LOCK.lock();
        let request_id = 100_003;
        start_async(
            request_id,
            Some(test_completion),
            Box::pin(async { panic!("test panic") }),
        )
        .expect("task should start");

        assert_eq!(take_notifications(), 1);
        let result = fdb_rs_async_poll(request_id);
        assert!(!result.is_null());
        assert_eq!(unsafe { fdb_rs_result_code(result) }, BRIDGE_PANIC);
        unsafe { fdb_rs_result_free(result) };
        assert_eq!(fdb_rs_async_ack(request_id), 0);
    }
}
