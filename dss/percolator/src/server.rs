use crate::msg::*;
use crate::service::*;
use crate::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future;
use labrpc::{Error, Result, RpcFuture};

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
}

impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    fn get_timestamp(&self, _: TimestampRequest) -> RpcFuture<TimestampResponse> {
        // Your code here.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("TimestampOracle::get_timestamp failed")
            .as_nanos();
        Box::new(future::result(Ok(TimestampResponse { ts: now as u64 })))
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

impl Value {
    fn unwrap_ts(&self) -> u64 {
        match self {
            Value::Timestamp(ts) => *ts,
            _ => panic!(),
        }
    }

    fn unwrap_vec(&self) -> Vec<u8> {
        match self {
            Value::Vector(v) => v.clone(),
            _ => panic!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let start_ts = match ts_start_inclusive {
            Some(ts) => ts,
            None => 0,
        };
        let end_ts = match ts_end_inclusive {
            Some(ts) => ts,
            None => std::u64::MAX,
        };
        match column {
            Column::Write => self
                .write
                .range((key.clone(), start_ts)..=(key, end_ts))
                .last(),
            Column::Data => self
                .data
                .range((key.clone(), start_ts)..=(key, end_ts))
                .last(),
            Column::Lock => self
                .lock
                .range((key.clone(), start_ts)..=(key, end_ts))
                .last(),
        }
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let key = (key, ts);
        match column {
            Column::Write => self.write.insert(key, value),
            Column::Data => self.data.insert(key, value),
            Column::Lock => self.lock.insert(key, value),
        };
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let table = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        let mut keys = vec![];
        for (k, _) in table.range(..=(key.clone(), commit_ts)) {
            if k.0 == key && k.1 <= commit_ts {
                keys.push(k.clone());
            }
        }
        for key in keys {
            table.remove(&key);
        }
    }

    #[inline]
    fn get_commit_ts(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        for (k, v) in self.write.range((key.clone(), start_ts)..) {
            if k.0 == key && v.unwrap_ts() == start_ts {
                return Some(k.1);
            }
        }
        None
    }

    #[inline]
    fn get_secondaries(&self, primary: Vec<u8>, lock_ts: u64) -> Vec<Key> {
        let mut secondaries = vec![];
        for (k, v) in self.lock.iter() {
            if k.1 == lock_ts && v.unwrap_vec() == primary {
                secondaries.push(k.clone());
            }
        }
        secondaries
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        // Your code here.
        let data = self.data.lock().unwrap();
        let (ts, key) = (req.ts, req.key);
        if data
            .read(key.clone(), Column::Lock, None, Some(ts))
            .is_some()
        {
            self.back_off_maybe_clean_up_lock(data, ts, key);
            return Box::new(future::result(Err(Error::Other("pending lock".to_owned()))));
        }
        let data_ts = match data.read(key.clone(), Column::Write, None, Some(ts)) {
            Some(res) => res.1.unwrap_ts(),
            None => return Box::new(future::result(Ok(GetResponse { value: vec![] }))),
        };
        let value = match data.read(key, Column::Data, Some(data_ts), Some(data_ts)) {
            Some(res) => res.1.unwrap_vec(),
            None => vec![],
        };
        Box::new(future::result(Ok(GetResponse { value: value })))
    }

    // example prewrite RPC handler.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        // Your code here.
        let mut data = self.data.lock().unwrap();
        let (start_ts, key, value, primary) = (req.start_ts, req.key, req.value, req.primary);
        if data
            .read(key.clone(), Column::Write, Some(start_ts), None)
            .is_some()
        {
            return Box::new(future::result(Err(Error::Other(
                "write conflict".to_owned(),
            ))));
        }
        if data.read(key.clone(), Column::Lock, None, None).is_some() {
            return Box::new(future::result(Err(Error::Other(
                "write conflict".to_owned(),
            ))));
        }
        data.write(key.clone(), Column::Data, start_ts, Value::Vector(value));
        data.write(key, Column::Lock, start_ts, Value::Vector(primary));
        Box::new(future::result(Ok(PrewriteResponse {})))
    }

    // example commit RPC handler.
    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        // Your code here.
        let mut data = self.data.lock().unwrap();
        let key = req.key;
        if req.is_primary {
            if data
                .read(
                    key.clone(),
                    Column::Lock,
                    Some(req.start_ts),
                    Some(req.start_ts),
                )
                .is_none()
            {
                return Box::new(future::result(Err(Error::Other(
                    "lock not found".to_owned(),
                ))));
            }
        }
        data.write(
            key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        data.erase(key, Column::Lock, req.commit_ts);
        Box::new(future::result(Ok(CommitResponse {})))
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock<'mutex>(
        &self,
        mut data: std::sync::MutexGuard<'mutex, KvTable>,
        start_ts: u64,
        key: Vec<u8>,
    ) {
        // Your code here.
        let pair = data
            .read(key.clone(), Column::Lock, None, Some(start_ts))
            .unwrap();
        let lock_ts = (pair.0).1;
        let curr_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("")
            .as_nanos() as u64;
        if curr_ts - lock_ts < TTL {
            return;
        }
        let primary = (pair.1).unwrap_vec();
        if let Some(primary_lock) =
            data.read(primary.clone(), Column::Lock, Some(lock_ts), Some(lock_ts))
        {
            assert!(primary_lock.1.unwrap_vec() == primary.clone());
            data.erase(primary.clone(), Column::Lock, lock_ts);
            data.erase(primary.clone(), Column::Data, lock_ts);
            // then rollback all secondary lock
            for secondary in data.get_secondaries(primary.clone(), lock_ts) {
                data.erase(secondary.0.clone(), Column::Lock, lock_ts);
                data.erase(secondary.0, Column::Data, lock_ts);
            }
        } else {
            if let Some(commit_ts) = data.get_commit_ts(primary.clone(), lock_ts) {
                for secondary in data.get_secondaries(primary.clone(), lock_ts) {
                    data.write(
                        secondary.0.clone(),
                        Column::Write,
                        commit_ts,
                        Value::Timestamp(lock_ts),
                    );
                    data.erase(secondary.0, Column::Lock, commit_ts);
                }
            } else {
                for secondary in data.get_secondaries(primary.clone(), lock_ts) {
                    data.erase(secondary.0.clone(), Column::Lock, lock_ts);
                    data.erase(secondary.0, Column::Data, lock_ts);
                }
            }
        }
    }
}
