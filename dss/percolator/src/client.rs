use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};

use std::time::Duration;

use futures::Future;
use futures_timer::Delay;
use labrpc::*;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    txn: Transaction,
}

#[derive(Default, Clone)]
pub struct Transaction {
    start_ts: u64,
    writes: Vec<Write>,
}

#[derive(Clone)]
struct Write(Vec<u8>, Vec<u8>);

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client: tso_client,
            txn_client: txn_client,
            txn: Default::default(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut backoff_time = BACKOFF_TIME_MS;
        for _ in 0..RETRY_TIMES {
            match self.tso_client.get_timestamp(&TimestampRequest {}).wait() {
                Ok(resp) => return Ok(resp.ts),
                Err(_) => {
                    let _ = Delay::new(Duration::from_millis(backoff_time)).wait();
                    backoff_time *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        self.txn.start_ts = self
            .get_timestamp()
            .expect("Client::begin get start timestamp failed");
        self.txn.writes = vec![];
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        assert!(self.txn.start_ts != 0);
        let mut backoff_time = BACKOFF_TIME_MS;
        for _ in 0..RETRY_TIMES {
            match self
                .txn_client
                .get(&GetRequest {
                    ts: self.txn.start_ts,
                    key: key.clone(),
                })
                .wait()
            {
                Ok(resp) => return Ok(resp.value),
                Err(_) => {
                    let _ = Delay::new(Duration::from_millis(backoff_time)).wait();
                    backoff_time *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        assert!(self.txn.start_ts != 0);
        self.txn.writes.push(Write(key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        assert!(self.txn.writes.len() != 0);
        let start_ts = self.txn.start_ts;
        let primary = &self.txn.writes[0];
        let secondaries = &self.txn.writes[1..];

        if self
            .txn_client
            .prewrite(&PrewriteRequest {
                start_ts: start_ts,
                key: primary.0.clone(),
                value: primary.1.clone(),
                primary: primary.0.clone(),
            })
            .wait()
            .is_err()
        {
            return Ok(false);
        }
        for write in secondaries {
            if self
                .txn_client
                .prewrite(&PrewriteRequest {
                    start_ts: start_ts,
                    key: write.0.clone(),
                    value: write.1.clone(),
                    primary: primary.0.clone(),
                })
                .wait()
                .is_err()
            {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;
        match self
            .txn_client
            .commit(&CommitRequest {
                is_primary: true,
                key: primary.0.clone(),
                start_ts: start_ts,
                commit_ts: commit_ts,
            })
            .wait()
        {
            Ok(_) => {}
            Err(Error::Other(e)) => {
                if e == "lock not found" || e == "reqhook" {
                    return Ok(false);
                } else {
                    return Err(Error::Other(e));
                }
            }
            Err(e) => return Err(e),
        };
        for write in secondaries {
            let _ = self
                .txn_client
                .commit(&CommitRequest {
                    is_primary: false,
                    key: write.0.clone(),
                    start_ts: start_ts,
                    commit_ts: commit_ts,
                })
                .wait();
        }
        Ok(true)
    }
}
