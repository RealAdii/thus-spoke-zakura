use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use bip39::Mnemonic;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use uuid::Uuid;
use zcash_keys::{
    address::Address,
    keys::{Era, UnifiedAddressRequest, UnifiedFullViewingKey, UnifiedSpendingKey},
};

use crate::wallet::regtest_network;

pub const ZATOSHIS_PER_ZEC: u64 = 100_000_000;
pub const USER_ACCOUNT_COUNT: u8 = 5;
pub const TREASURY_ACCOUNT_ID: u8 = 6;
/// The order must match `row_account`.
const ACCOUNT_COLUMNS: &str =
    "id,name,unified_address,transparent_address,transparent_zatoshi,ironwood_zatoshi";
/// Qualified so it also reads unambiguously in joins. The order must match `row_activity`.
const ACTIVITY_COLUMNS: &str = "a.id,a.kind,a.from_account,a.to_account,a.to_address,a.source_pool,a.destination_pool,a.amount_zatoshi,a.txid,a.block_hash,a.status,a.created_at,a.last_error";
/// The order must match `row_address_faucet`.
const ADDRESS_FAUCET_COLUMNS: &str = "id,address,amount_zatoshi,txid,block_hash,status,last_error";

#[derive(Debug, thiserror::Error)]
#[error("idempotency key was already used for a different payment")]
pub struct IdempotencyConflict;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreasuryCursor {
    pub receiver: String,
    pub height: u32,
    pub block_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Account {
    pub id: u8,
    pub name: String,
    pub unified_address: String,
    pub transparent_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unified_full_viewing_key: Option<String>,
    pub transparent_zatoshi: u64,
    pub ironwood_zatoshi: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevelopmentAccountSecret {
    pub id: u8,
    pub unified_address: String,
    pub unified_spending_key_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevelopmentSecrets {
    pub mnemonic: String,
    pub accounts: Vec<DevelopmentAccountSecret>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Activity {
    pub id: String,
    pub kind: String,
    pub from_account: Option<u8>,
    pub to_account: Option<u8>,
    pub to_address: Option<String>,
    pub source_pool: String,
    pub destination_pool: String,
    pub amount_zatoshi: u64,
    pub txid: String,
    pub block_hash: Option<String>,
    pub status: String,
    pub created_at: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressFaucet {
    pub id: String,
    pub address: String,
    pub amount_zatoshi: u64,
    pub txid: String,
    pub block_hash: Option<String>,
    pub status: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityIntent {
    pub activity: Activity,
    pub key: String,
    pub memo: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressFaucetIntent {
    pub payment: AddressFaucet,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTransaction {
    pub raw_transaction: Vec<u8>,
    pub expiry_height: u64,
}

/// Where a send is going: a development account, or any other Regtest address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recipient<'a> {
    Account(u8),
    Address(&'a str),
}

#[derive(Clone)]
pub struct Store(Arc<Mutex<Connection>>);

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    pub fn initialize(&self) -> Result<()> {
        let db = self.0.lock().unwrap();
        db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY, name TEXT NOT NULL, unified_address TEXT NOT NULL,
                transparent_address TEXT NOT NULL, transparent_zatoshi INTEGER NOT NULL DEFAULT 0,
                ironwood_zatoshi INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS idempotency (
                key TEXT PRIMARY KEY, activity_id TEXT NOT NULL, memo TEXT
            );
            CREATE TABLE IF NOT EXISTS prepared_payments (
                activity_id TEXT PRIMARY KEY, raw_transaction BLOB NOT NULL,
                expiry_height INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS address_faucets (
                key TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, address TEXT NOT NULL,
                amount_zatoshi INTEGER NOT NULL, txid TEXT NOT NULL DEFAULT '',
                block_hash TEXT, status TEXT NOT NULL DEFAULT 'preparing', last_error TEXT
            );
        "#,
        )?;
        db.execute_batch(ACTIVITY_TABLE)?;
        add_missing_column(&db, "idempotency", "memo")?;
        add_missing_column(&db, "activity", "last_error")?;
        add_missing_column(&db, "address_faucets", "last_error")?;
        migrate_activity_recipients(&db)?;
        let stored_seed = db
            .query_row("SELECT value FROM metadata WHERE key='seed'", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()?;
        let seed = if let Some(seed) = stored_seed {
            hex::decode(seed).context("invalid wallet seed")?
        } else {
            let entropy = [0u8; 32];
            let mnemonic = Mnemonic::from_entropy(&entropy)
                .context("encoding wallet entropy as a BIP-39 mnemonic")?;
            let seed = mnemonic.to_seed("");
            db.execute(
                "INSERT INTO metadata(key, value) VALUES('seed', ?1)",
                [hex::encode(seed)],
            )?;
            db.execute(
                "INSERT INTO metadata(key, value) VALUES('mnemonic', ?1)",
                [mnemonic.to_string()],
            )?;
            seed.to_vec()
        };
        for id in 1u8..=TREASURY_ACCOUNT_ID {
            let (ua, taddr) = derived_addresses(&seed, id)?;
            db.execute("INSERT OR IGNORE INTO accounts(id,name,unified_address,transparent_address) VALUES(?1,?2,?3,?4)", params![id, format!("Account {id}"), ua, taddr])?;
        }
        Ok(())
    }

    pub fn accounts(&self) -> Result<Vec<Account>> {
        let mut accounts = {
            let db = self.0.lock().unwrap();
            let mut query = db.prepare(&format!(
                "SELECT {ACCOUNT_COLUMNS} FROM accounts ORDER BY id"
            ))?;
            query
                .query_map([], row_account)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let seed = hex::decode(self.seed()?).context("invalid wallet seed")?;
        let network = regtest_network();
        for account in &mut accounts {
            if (1..=USER_ACCOUNT_COUNT).contains(&account.id) {
                account.unified_full_viewing_key =
                    Some(derived_full_viewing_key(&seed, account.id)?.encode(&network));
            }
        }
        Ok(accounts)
    }

    #[cfg(test)]
    pub fn user_accounts(&self) -> Result<Vec<Account>> {
        Ok(self
            .accounts()?
            .into_iter()
            .filter(|account| account.id <= USER_ACCOUNT_COUNT)
            .collect())
    }

    pub fn account(&self, id: u8) -> Result<Account> {
        self.0
            .lock()
            .unwrap()
            .query_row(
                &format!("SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id=?1"),
                [id],
                row_account,
            )
            .with_context(|| format!("account {id} does not exist"))
    }

    pub fn activities(&self, limit: u32) -> Result<Vec<Activity>> {
        let db = self.0.lock().unwrap();
        let mut query = db.prepare(&format!(
            "SELECT {ACTIVITY_COLUMNS} FROM activity a ORDER BY a.rowid DESC LIMIT ?1"
        ))?;
        Ok(query
            .query_map([limit.min(100)], row_activity)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn unconfirmed_activities(&self) -> Result<Vec<Activity>> {
        let db = self.0.lock().unwrap();
        let mut query = db.prepare(&format!(
            "SELECT {ACTIVITY_COLUMNS} FROM activity a WHERE a.txid!='' AND a.status NOT IN ('confirmed','failed') ORDER BY a.rowid ASC"
        ))?;
        Ok(query
            .query_map([], row_activity)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn unresolved_activities(&self) -> Result<Vec<ActivityIntent>> {
        let db = self.0.lock().unwrap();
        let mut query = db.prepare(&format!(
            "SELECT {ACTIVITY_COLUMNS},i.key,i.memo FROM activity a JOIN idempotency i ON i.activity_id=a.id WHERE a.status NOT IN ('confirmed','failed') ORDER BY a.rowid ASC"
        ))?;
        Ok(query
            .query_map([], |row| {
                Ok(ActivityIntent {
                    activity: row_activity(row)?,
                    key: row.get(13)?,
                    memo: row.get(14)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn unresolved_address_faucets(&self) -> Result<Vec<AddressFaucetIntent>> {
        let db = self.0.lock().unwrap();
        let mut query = db.prepare(&format!(
            "SELECT {ADDRESS_FAUCET_COLUMNS},key FROM address_faucets WHERE status NOT IN ('confirmed','failed') ORDER BY rowid ASC"
        ))?;
        Ok(query
            .query_map([], |row| {
                Ok(AddressFaucetIntent {
                    payment: row_address_faucet(row)?,
                    key: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn activity_for_key(&self, key: &str) -> Result<Option<Activity>> {
        let db = self.0.lock().unwrap();
        activity_for_key(&db, key)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_transfer(
        &self,
        from: u8,
        to: Recipient<'_>,
        source_pool: &str,
        destination_pool: &str,
        amount: u64,
        key: &str,
        memo: Option<&str>,
    ) -> Result<Activity> {
        validate_pool(source_pool)?;
        validate_pool(destination_pool)?;
        let request = new_activity(
            "send",
            Some(from),
            to,
            source_pool,
            destination_pool,
            amount,
        );
        self.claim(request, key, memo)
    }

    pub fn claim_faucet(&self, to: u8, pool: &str, amount: u64, key: &str) -> Result<Activity> {
        validate_pool(pool)?;
        let request = new_activity(
            "faucet",
            None,
            Recipient::Account(to),
            "ironwood",
            pool,
            amount,
        );
        self.claim(request, key, None)
    }

    /// Returns the payment already claimed with `key`, or records `request` as a new
    /// `preparing` claim. Submission and recovery happen outside this function.
    fn claim(&self, request: Activity, key: &str, memo: Option<&str>) -> Result<Activity> {
        if request.amount_zatoshi == 0 {
            bail!("amount must be greater than zero");
        }
        // Hold the lock from lookup to insert so concurrent retries create one payment.
        let mut db = self.0.lock().unwrap();
        if let Some(activity) = activity_for_key(&db, key)? {
            let saved_memo: Option<String> =
                db.query_row("SELECT memo FROM idempotency WHERE key=?1", [key], |row| {
                    row.get(0)
                })?;
            // An absent memo and an empty memo are different payments.
            if !same_payment(&activity, &request) || saved_memo.as_deref() != memo {
                return Err(IdempotencyConflict.into());
            }
            return Ok(activity);
        }
        if address_for_key(&db, key)?.is_some() {
            return Err(IdempotencyConflict.into());
        }
        for id in request.from_account.into_iter().chain(request.to_account) {
            if !db.query_row(
                "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=?1)",
                [id],
                |row| row.get::<_, bool>(0),
            )? {
                bail!("account {id} does not exist");
            }
        }
        let tx = db.transaction()?;
        insert_activity(&tx, &request, key, memo)?;
        tx.commit()?;
        Ok(request)
    }

    pub fn address_faucet_for_key(&self, key: &str) -> Result<Option<AddressFaucet>> {
        let db = self.0.lock().unwrap();
        address_for_key(&db, key)
    }

    pub fn claim_address_faucet(
        &self,
        address: &str,
        amount: u64,
        key: &str,
    ) -> Result<AddressFaucet> {
        if amount == 0 {
            bail!("amount must be greater than zero");
        }
        let db = self.0.lock().unwrap();
        if let Some(payment) = address_for_key(&db, key)? {
            if payment.address != address || payment.amount_zatoshi != amount {
                return Err(IdempotencyConflict.into());
            }
            return Ok(payment);
        }
        if activity_for_key(&db, key)?.is_some() {
            return Err(IdempotencyConflict.into());
        }
        let id = Uuid::new_v4().to_string();
        db.execute(
            "INSERT INTO address_faucets(key,id,address,amount_zatoshi) VALUES(?1,?2,?3,?4)",
            params![key, id, address, amount],
        )?;
        address_for_key(&db, key)?.context("address faucet claim was not stored")
    }

    #[cfg(test)]
    pub fn discard_address_preparing(&self, id: &str) -> Result<()> {
        self.0.lock().unwrap().execute(
            "DELETE FROM address_faucets WHERE id=?1 AND status='preparing'",
            [id],
        )?;
        Ok(())
    }

    pub fn record_address_prepared(
        &self,
        id: &str,
        txid: &str,
        raw_transaction: &[u8],
        expiry_height: u64,
    ) -> Result<AddressFaucet> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let updated = tx.execute(
            "UPDATE address_faucets SET txid=?1,status='prepared',last_error=NULL WHERE id=?2 AND status='preparing'",
            params![txid, id],
        )?;
        let payment = address_by_id(&tx, id)?;
        if updated == 0
            && (payment.txid != txid
                || !matches!(
                    payment.status.as_str(),
                    "prepared" | "broadcast" | "confirmed"
                ))
        {
            bail!("address faucet {id} is not waiting for this prepared transaction");
        }
        tx.execute(
            "INSERT INTO prepared_payments(activity_id,raw_transaction,expiry_height) VALUES(?1,?2,?3) ON CONFLICT(activity_id) DO NOTHING",
            params![id, raw_transaction, expiry_height],
        )?;
        tx.commit()?;
        Ok(payment)
    }

    pub fn reset_address_for_retry(&self, id: &str, txid: &str) -> Result<AddressFaucet> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        if tx.execute(
            "UPDATE address_faucets SET txid='',block_hash=NULL,status='preparing' WHERE id=?1 AND txid=?2 AND status IN ('prepared','broadcast')",
            params![id, txid],
        )? == 1 {
            tx.execute("DELETE FROM prepared_payments WHERE activity_id=?1", [id])?;
        }
        let payment = address_by_id(&tx, id)?;
        tx.commit()?;
        Ok(payment)
    }

    pub fn mark_address_broadcast(&self, id: &str, txid: &str) -> Result<AddressFaucet> {
        let db = self.0.lock().unwrap();
        let updated = db.execute(
            "UPDATE address_faucets SET status='broadcast',last_error=NULL WHERE id=?1 AND txid=?2 AND status IN ('prepared','broadcast')",
            params![id, txid],
        )?;
        let payment = address_by_id(&db, id)?;
        if updated == 0 && (payment.txid != txid || payment.status != "confirmed") {
            bail!("address faucet {id} has no matching prepared transaction");
        }
        Ok(payment)
    }

    pub fn confirm_address(&self, id: &str, txid: &str, block_hash: &str) -> Result<AddressFaucet> {
        if block_hash.is_empty() {
            bail!("block hash is required to confirm address faucet");
        }
        let db = self.0.lock().unwrap();
        db.execute(
            "UPDATE address_faucets SET status='confirmed',block_hash=?1,last_error=NULL WHERE id=?2 AND txid=?3",
            params![block_hash, id, txid],
        )?;
        address_by_id(&db, id)
    }

    pub fn record_address_error(
        &self,
        id: &str,
        status: &str,
        txid: &str,
        error: &str,
    ) -> Result<AddressFaucet> {
        let db = self.0.lock().unwrap();
        db.execute(
            "UPDATE address_faucets SET last_error=?1 WHERE id=?2 AND status=?3 AND txid=?4 AND status NOT IN ('confirmed','failed')",
            params![error, id, status, txid],
        )?;
        address_by_id(&db, id)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "server-owned payment progression (#196) uses this"
        )
    )]
    pub fn fail_address(&self, id: &str, error: &str) -> Result<AddressFaucet> {
        let db = self.0.lock().unwrap();
        let updated = db.execute(
            "UPDATE address_faucets SET status='failed',last_error=?1 WHERE id=?2 AND status='preparing' AND txid=''",
            params![error, id],
        )?;
        let payment = address_by_id(&db, id)?;
        if updated == 0 && payment.status != "failed" {
            bail!("address faucet {id} is no longer waiting for preparation");
        }
        Ok(payment)
    }

    pub fn record_error(
        &self,
        id: &str,
        status: &str,
        txid: &str,
        error: &str,
    ) -> Result<Activity> {
        let db = self.0.lock().unwrap();
        db.execute(
            "UPDATE activity SET last_error=?1 WHERE id=?2 AND status=?3 AND txid=?4 AND status NOT IN ('confirmed','failed')",
            params![error, id, status, txid],
        )?;
        activity_by_id(&db, id).map_err(Into::into)
    }

    pub fn fail(&self, id: &str, error: &str) -> Result<Activity> {
        let db = self.0.lock().unwrap();
        let updated = db.execute(
            "UPDATE activity SET status='failed',last_error=?1 WHERE id=?2 AND status='preparing' AND txid=''",
            params![error, id],
        )?;
        let activity = activity_by_id(&db, id)?;
        if updated == 0 && activity.status != "failed" {
            bail!("payment {id} is no longer waiting for preparation");
        }
        Ok(activity)
    }

    #[cfg(test)]
    pub fn discard_preparing(&self, id: &str) -> Result<()> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        if tx.execute(
            "DELETE FROM activity WHERE id=?1 AND status='preparing'",
            [id],
        )? == 1
        {
            tx.execute("DELETE FROM idempotency WHERE activity_id=?1", [id])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn record_prepared(
        &self,
        id: &str,
        txid: &str,
        raw_transaction: &[u8],
        expiry_height: u64,
    ) -> Result<Activity> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let updated = tx.execute(
            "UPDATE activity SET txid=?1,status='prepared',last_error=NULL WHERE id=?2 AND status='preparing'",
            params![txid, id],
        )?;
        let activity = activity_by_id(&tx, id)?;
        if updated == 0
            && (activity.txid != txid
                || !matches!(
                    activity.status.as_str(),
                    "prepared" | "broadcast" | "confirmed"
                ))
        {
            bail!("payment {id} is not waiting for this prepared transaction");
        }
        tx.execute(
            "INSERT INTO prepared_payments(activity_id,raw_transaction,expiry_height) VALUES(?1,?2,?3) ON CONFLICT(activity_id) DO NOTHING",
            params![id, raw_transaction, expiry_height],
        )?;
        tx.commit()?;
        Ok(activity)
    }

    pub fn prepared_transaction(&self, id: &str) -> Result<PreparedTransaction> {
        self.0
            .lock()
            .unwrap()
            .query_row(
                "SELECT raw_transaction,expiry_height FROM prepared_payments WHERE activity_id=?1",
                [id],
                |row| {
                    Ok(PreparedTransaction {
                        raw_transaction: row.get(0)?,
                        expiry_height: row.get(1)?,
                    })
                },
            )
            .with_context(|| format!("prepared transaction {id} is missing"))
    }

    pub fn prepared_transaction_expiry(&self, id: &str) -> Result<Option<u64>> {
        self.0
            .lock()
            .unwrap()
            .query_row(
                "SELECT expiry_height FROM prepared_payments WHERE activity_id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn reset_for_retry(&self, id: &str, txid: &str) -> Result<Activity> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let updated = tx.execute(
            "UPDATE activity SET txid='',block_hash=NULL,status='preparing' WHERE id=?1 AND txid=?2 AND status IN ('prepared','broadcast')",
            params![id, txid],
        )?;
        if updated == 1 {
            tx.execute("DELETE FROM prepared_payments WHERE activity_id=?1", [id])?;
        }
        let activity = activity_by_id(&tx, id)?;
        tx.commit()?;
        Ok(activity)
    }

    pub fn mark_broadcast(&self, id: &str, txid: &str) -> Result<Activity> {
        let db = self.0.lock().unwrap();
        let updated = db.execute(
            "UPDATE activity SET status='broadcast',last_error=NULL WHERE id=?1 AND txid=?2 AND status IN ('prepared','broadcast')",
            params![id, txid],
        )?;
        let activity = activity_by_id(&db, id)?;
        if updated == 0 && (activity.txid != txid || activity.status != "confirmed") {
            bail!("payment {id} has no matching prepared transaction");
        }
        Ok(activity)
    }

    pub fn confirm(&self, id: &str, txid: &str, block_hash: &str) -> Result<Activity> {
        if block_hash.is_empty() {
            bail!("block hash is required to confirm activity");
        }
        let db = self.0.lock().unwrap();
        db.execute(
            "UPDATE activity SET status='confirmed',block_hash=?1,last_error=NULL WHERE id=?2 AND txid=?3",
            params![block_hash, id, txid],
        )?;
        activity_by_id(&db, id).map_err(Into::into)
    }

    pub fn seed(&self) -> Result<String> {
        Ok(self.0.lock().unwrap().query_row(
            "SELECT value FROM metadata WHERE key='seed'",
            [],
            |r| r.get(0),
        )?)
    }

    pub fn mnemonic(&self) -> Result<String> {
        self.0
            .lock()
            .unwrap()
            .query_row("SELECT value FROM metadata WHERE key='mnemonic'", [], |r| {
                r.get(0)
            })
            .context("wallet mnemonic is missing; restart this disposable environment")
    }

    pub fn development_secrets(&self) -> Result<DevelopmentSecrets> {
        let seed = hex::decode(self.seed()?).context("invalid wallet seed")?;
        let mnemonic = self.mnemonic()?;
        let network = regtest_network();
        let mut accounts = Vec::with_capacity(usize::from(USER_ACCOUNT_COUNT));
        for id in 1..=USER_ACCOUNT_COUNT {
            let account_index = zip32::AccountId::try_from(u32::from(id - 1))
                .map_err(|_| anyhow::anyhow!("invalid ZIP-32 account {id}"))?;
            let spending_key = UnifiedSpendingKey::from_seed(&network, &seed, account_index)
                .map_err(|error| anyhow::anyhow!("deriving account {id}: {error:?}"))?;
            accounts.push(DevelopmentAccountSecret {
                id,
                unified_address: self.account(id)?.unified_address,
                unified_spending_key_hex: hex::encode(spending_key.to_bytes(Era::Orchard)),
            });
        }
        Ok(DevelopmentSecrets { mnemonic, accounts })
    }
}

fn add_missing_column(db: &Connection, table: &str, column: &str) -> Result<()> {
    let exists = db
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|existing| existing == column);
    if !exists {
        db.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"), [])?;
    }
    Ok(())
}

const ACTIVITY_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS activity (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_account INTEGER, to_account INTEGER,
        to_address TEXT, source_pool TEXT NOT NULL, destination_pool TEXT NOT NULL,
        amount_zatoshi INTEGER NOT NULL, txid TEXT NOT NULL, block_hash TEXT, status TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, last_error TEXT
    );
"#;

/// Databases created before address sends have `to_account NOT NULL` and no
/// `to_address`. SQLite cannot relax a constraint in place, so rebuild the
/// table, keeping rowid order because activity is listed by rowid.
fn migrate_activity_recipients(db: &Connection) -> Result<()> {
    let migrated: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('activity') WHERE name='to_address')",
        [],
        |r| r.get(0),
    )?;
    if migrated {
        return Ok(());
    }
    let tx = db.unchecked_transaction()?;
    tx.execute_batch("ALTER TABLE activity RENAME TO activity_before_recipients;")?;
    tx.execute_batch(ACTIVITY_TABLE)?;
    tx.execute_batch(
        "INSERT INTO activity(id,kind,from_account,to_account,source_pool,destination_pool,amount_zatoshi,txid,block_hash,status,created_at,last_error)
         SELECT id,kind,from_account,to_account,source_pool,destination_pool,amount_zatoshi,txid,block_hash,status,created_at,last_error
         FROM activity_before_recipients ORDER BY rowid;
         DROP TABLE activity_before_recipients;",
    )?;
    tx.commit()?;
    Ok(())
}

fn insert_activity(db: &Connection, a: &Activity, key: &str, memo: Option<&str>) -> Result<()> {
    db.execute("INSERT INTO activity(id,kind,from_account,to_account,to_address,source_pool,destination_pool,amount_zatoshi,txid,status) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)", params![a.id,a.kind,a.from_account,a.to_account,a.to_address,a.source_pool,a.destination_pool,a.amount_zatoshi,a.txid,a.status])?;
    db.execute(
        "INSERT INTO idempotency(key,activity_id,memo) VALUES(?1,?2,?3)",
        params![key, a.id, memo],
    )?;
    Ok(())
}

fn activity_by_id(db: &Connection, id: &str) -> rusqlite::Result<Activity> {
    db.query_row(
        &format!("SELECT {ACTIVITY_COLUMNS} FROM activity a WHERE a.id=?1"),
        [id],
        row_activity,
    )
}

fn activity_for_key(db: &Connection, key: &str) -> Result<Option<Activity>> {
    db.query_row(
        &format!(
            "SELECT {ACTIVITY_COLUMNS} FROM activity a JOIN idempotency i ON i.activity_id=a.id WHERE i.key=?1"
        ),
        [key],
        row_activity,
    )
    .optional()
    .map_err(Into::into)
}
fn address_for_key(db: &Connection, key: &str) -> Result<Option<AddressFaucet>> {
    db.query_row(
        &format!("SELECT {ADDRESS_FAUCET_COLUMNS} FROM address_faucets WHERE key=?1"),
        [key],
        row_address_faucet,
    )
    .optional()
    .map_err(Into::into)
}
fn address_by_id(db: &Connection, id: &str) -> Result<AddressFaucet> {
    db.query_row(
        &format!("SELECT {ADDRESS_FAUCET_COLUMNS} FROM address_faucets WHERE id=?1"),
        [id],
        row_address_faucet,
    )
    .map_err(Into::into)
}
fn row_address_faucet(row: &rusqlite::Row<'_>) -> rusqlite::Result<AddressFaucet> {
    Ok(AddressFaucet {
        id: row.get(0)?,
        address: row.get(1)?,
        amount_zatoshi: row.get(2)?,
        txid: row.get(3)?,
        block_hash: row.get(4)?,
        status: row.get(5)?,
        last_error: row.get(6)?,
    })
}
fn new_activity(
    kind: &str,
    from: Option<u8>,
    to: Recipient<'_>,
    source: &str,
    destination: &str,
    amount: u64,
) -> Activity {
    let id = Uuid::new_v4().to_string();
    Activity {
        id,
        kind: kind.into(),
        from_account: from,
        to_account: match to {
            Recipient::Account(id) => Some(id),
            Recipient::Address(_) => None,
        },
        to_address: match to {
            Recipient::Account(_) => None,
            Recipient::Address(address) => Some(address.to_owned()),
        },
        source_pool: source.into(),
        destination_pool: destination.into(),
        amount_zatoshi: amount,
        txid: String::new(),
        block_hash: None,
        status: "preparing".into(),
        created_at: String::new(),
        last_error: None,
    }
}
fn validate_pool(pool: &str) -> Result<()> {
    if matches!(pool, "transparent" | "ironwood") {
        Ok(())
    } else {
        bail!("pool must be transparent or ironwood")
    }
}

fn same_payment(existing: &Activity, request: &Activity) -> bool {
    existing.kind == request.kind
        && existing.from_account == request.from_account
        && existing.to_account == request.to_account
        && existing.to_address == request.to_address
        && existing.source_pool == request.source_pool
        && existing.destination_pool == request.destination_pool
        && existing.amount_zatoshi == request.amount_zatoshi
}
fn row_account(row: &rusqlite::Row<'_>) -> rusqlite::Result<Account> {
    Ok(Account {
        id: row.get(0)?,
        name: row.get(1)?,
        unified_address: row.get(2)?,
        transparent_address: row.get(3)?,
        unified_full_viewing_key: None,
        transparent_zatoshi: row.get(4)?,
        ironwood_zatoshi: row.get(5)?,
    })
}
fn row_activity(row: &rusqlite::Row<'_>) -> rusqlite::Result<Activity> {
    Ok(Activity {
        id: row.get(0)?,
        kind: row.get(1)?,
        from_account: row.get(2)?,
        to_account: row.get(3)?,
        to_address: row.get(4)?,
        source_pool: row.get(5)?,
        destination_pool: row.get(6)?,
        amount_zatoshi: row.get(7)?,
        txid: row.get(8)?,
        block_hash: row.get(9)?,
        status: row.get(10)?,
        created_at: row.get(11)?,
        last_error: row.get(12)?,
    })
}
fn derived_full_viewing_key(seed: &[u8], id: u8) -> Result<UnifiedFullViewingKey> {
    let index = id.checked_sub(1).context("invalid account id")?;
    let account = zip32::AccountId::try_from(u32::from(index))
        .map_err(|_| anyhow::anyhow!("invalid ZIP-32 account {id}"))?;
    let usk = UnifiedSpendingKey::from_seed(&regtest_network(), seed, account)
        .map_err(|error| anyhow::anyhow!("deriving account {id}: {error:?}"))?;
    Ok(usk.to_unified_full_viewing_key())
}

fn derived_addresses(seed: &[u8], id: u8) -> Result<(String, String)> {
    let network = regtest_network();
    let (ua, _) = derived_full_viewing_key(seed, id)?
        .default_address(UnifiedAddressRequest::AllAvailableKeys)
        .map_err(|error| anyhow::anyhow!("deriving account {id} address: {error:?}"))?;
    let transparent = ua
        .transparent()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("account {id} has no transparent receiver"))?;
    Ok((
        ua.encode(&network),
        Address::Transparent(transparent).encode(&network),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    #[test]
    fn address_faucet_replay_survives_restart_and_rejects_conflicting_intents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let address = store.account(1).unwrap().unified_address;
        let first = store
            .claim_address_faucet(&address, 100_000_000, "address-key")
            .unwrap();
        let first = store
            .record_address_prepared(&first.id, "txid-one", b"signed bytes", 140)
            .unwrap();
        assert_eq!(first.status, "prepared");
        drop(store);

        let reopened = Store::open(&path).unwrap();
        reopened.initialize().unwrap();
        let replay = reopened
            .claim_address_faucet(&address, 100_000_000, "address-key")
            .unwrap();
        assert_eq!(replay.id, first.id);
        assert_eq!(replay.txid, "txid-one");
        assert_eq!(
            reopened
                .prepared_transaction(&replay.id)
                .unwrap()
                .raw_transaction,
            b"signed bytes"
        );
        assert!(
            reopened
                .claim_address_faucet(&address, 2, "address-key")
                .is_err()
        );
        assert!(
            reopened
                .claim_address_faucet(
                    &reopened.account(2).unwrap().unified_address,
                    100_000_000,
                    "address-key"
                )
                .is_err()
        );
        assert!(
            reopened
                .claim_faucet(1, "ironwood", 100_000_000, "address-key")
                .is_err()
        );
        assert!(
            reopened
                .claim_address_faucet(&address, 100_000_000, "fresh-key")
                .is_ok()
        );
        reopened
            .claim_faucet(1, "ironwood", 100_000_000, "account-key")
            .unwrap();
        assert!(
            reopened
                .claim_address_faucet(&address, 100_000_000, "account-key")
                .is_err()
        );
    }
    #[test]
    fn concurrent_address_faucet_claims_share_one_operation() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let claims: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.claim_address_faucet("same-address", 1, "same-operation")
                })
            })
            .collect();
        barrier.wait();
        let ids: Vec<_> = claims
            .into_iter()
            .map(|claim| claim.join().unwrap().unwrap().id)
            .collect();
        assert_eq!(ids[0], ids[1]);
    }
    #[test]
    fn creates_user_accounts_and_hidden_treasury() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        assert_eq!(store.accounts().unwrap().len(), 6);
        assert_eq!(store.user_accounts().unwrap().len(), 5);
        assert_eq!(
            store.account(TREASURY_ACCOUNT_ID).unwrap().name,
            "Account 6"
        );
        assert!(
            store
                .account(1)
                .unwrap()
                .transparent_address
                .starts_with("tm")
        );
        let first = store
            .claim_faucet(2, "ironwood", ZATOSHIS_PER_ZEC, "same")
            .unwrap();
        let second = store
            .claim_faucet(2, "ironwood", ZATOSHIS_PER_ZEC, "same")
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(store.account(2).unwrap().ironwood_zatoshi, 0);
    }
    #[test]
    fn fresh_stores_derive_the_same_development_accounts() {
        let first = Store::open(":memory:").unwrap();
        first.initialize().unwrap();
        let second = Store::open(":memory:").unwrap();
        second.initialize().unwrap();
        assert_eq!(first.accounts().unwrap(), second.accounts().unwrap());
        assert_eq!(
            first.mnemonic().unwrap(),
            format!("{}art", "abandon ".repeat(23))
        );
        assert_eq!(
            first.account(1).unwrap().transparent_address,
            "tmBsTi2xWTjUdEXnuTceL7fecEQKeWaPDJd"
        );
    }
    #[test]
    fn restores_the_hidden_treasury_for_existing_stores() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        store
            .0
            .lock()
            .unwrap()
            .execute("DELETE FROM accounts WHERE id=?1", [TREASURY_ACCOUNT_ID])
            .unwrap();
        store.initialize().unwrap();
        assert_eq!(store.accounts().unwrap().len(), 6);
        assert_eq!(store.user_accounts().unwrap().len(), 5);
    }
    #[test]
    fn existing_store_exports_user_viewing_keys_without_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let seed = [7u8; 32];
        {
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE accounts (
                     id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                     unified_address TEXT NOT NULL, transparent_address TEXT NOT NULL,
                     transparent_zatoshi INTEGER NOT NULL DEFAULT 0,
                     ironwood_zatoshi INTEGER NOT NULL DEFAULT 0
                 );",
            )
            .unwrap();
            db.execute(
                "INSERT INTO metadata(key,value) VALUES('seed',?1)",
                [hex::encode(seed)],
            )
            .unwrap();
            for id in 1..=TREASURY_ACCOUNT_ID {
                let (ua, taddr) = derived_addresses(&seed, id).unwrap();
                db.execute(
                    "INSERT INTO accounts
                     (id,name,unified_address,transparent_address,transparent_zatoshi,ironwood_zatoshi)
                     VALUES(?1,?2,?3,?4,123,456)",
                    params![id, format!("Account {id}"), ua, taddr],
                )
                .unwrap();
            }
        }

        let store = Store::open(&path).unwrap();
        let users = store.user_accounts().unwrap();
        assert_eq!(
            users.iter().map(|a| a.id).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        let network = regtest_network();
        let mut distinct = std::collections::HashSet::new();
        for account in &users {
            let json = serde_json::to_value(account).unwrap();
            let encoded = json["unified_full_viewing_key"]
                .as_str()
                .expect("user projection must contain a viewing key");
            let viewing = zcash_keys::keys::UnifiedFullViewingKey::decode(&network, encoded)
                .unwrap_or_else(|_| panic!("user viewing key must decode for regtest"));
            let (ua, _) = viewing
                .default_address(UnifiedAddressRequest::AllAvailableKeys)
                .unwrap();
            assert!(ua.encode(&network) == account.unified_address);
            let transparent = ua.transparent().cloned().expect("transparent receiver");
            assert!(
                Address::Transparent(transparent).encode(&network) == account.transparent_address
            );
            let index = zip32::AccountId::try_from(u32::from(account.id - 1)).unwrap();
            let expected = UnifiedSpendingKey::from_seed(&network, &seed, index)
                .unwrap()
                .to_unified_full_viewing_key()
                .encode(&network);
            assert!(encoded == expected);
            assert!(distinct.insert(encoded.to_owned()));
            assert_eq!(account.transparent_zatoshi, 123);
            assert_eq!(account.ironwood_zatoshi, 456);
        }

        let all = store.accounts().unwrap();
        assert_eq!(all.len(), 6);
        let treasury = all.iter().find(|a| a.id == TREASURY_ACCOUNT_ID).unwrap();
        assert!(
            serde_json::to_value(treasury)
                .unwrap()
                .get("unified_full_viewing_key")
                .is_none()
        );
        assert!(
            serde_json::to_value(store.account(TREASURY_ACCOUNT_ID).unwrap())
                .unwrap()
                .get("unified_full_viewing_key")
                .is_none()
        );
        store.initialize().unwrap();
        assert!(users == store.user_accounts().unwrap());
        {
            let db = store.0.lock().unwrap();
            let columns = db
                .prepare("PRAGMA table_info(accounts)")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                columns,
                [
                    "id",
                    "name",
                    "unified_address",
                    "transparent_address",
                    "transparent_zatoshi",
                    "ironwood_zatoshi",
                ]
            );
            let metadata_keys = db
                .prepare("SELECT key FROM metadata ORDER BY key")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(metadata_keys, ["seed"]);
        }
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert!(users == reopened.user_accounts().unwrap());
    }
    #[test]
    fn records_real_transfer_without_mutating_balances() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let activity = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "send",
                None,
            )
            .unwrap();
        let activity = store
            .record_prepared(&activity.id, "real-txid", b"raw transaction", 140)
            .unwrap();
        let activity = store.mark_broadcast(&activity.id, &activity.txid).unwrap();
        assert_eq!(activity.txid, "real-txid");
        assert_eq!(store.account(1).unwrap().transparent_zatoshi, 0);
    }

    #[test]
    fn unconfirmed_activities_skip_unprepared_and_confirmed_rows() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                11_000,
                "preparing-key",
                None,
            )
            .unwrap();
        let pending = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "pending-key",
                None,
            )
            .unwrap();
        let pending = store
            .record_prepared(&pending.id, "txid-pending", b"pending", 140)
            .unwrap();
        let pending = store.mark_broadcast(&pending.id, &pending.txid).unwrap();
        let mined = store
            .claim_transfer(
                1,
                Recipient::Account(3),
                "ironwood",
                "ironwood",
                13_000,
                "mined-key",
                None,
            )
            .unwrap();
        let mined = store
            .record_prepared(&mined.id, "txid-mined", b"mined", 140)
            .unwrap();
        let mined = store.mark_broadcast(&mined.id, &mined.txid).unwrap();
        store
            .confirm(&mined.id, &mined.txid, &"c".repeat(64))
            .unwrap();

        let open = store.unconfirmed_activities().unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, pending.id);
        assert_eq!(open[0].status, "broadcast");
        assert_eq!(open[0].txid, "txid-pending");
    }

    #[test]
    fn confirm_rejects_an_empty_block_hash() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let pending = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "empty-hash",
                None,
            )
            .unwrap();
        let pending = store
            .record_prepared(&pending.id, "txid-empty", b"raw", 140)
            .unwrap();
        let pending = store.mark_broadcast(&pending.id, &pending.txid).unwrap();

        assert!(store.confirm(&pending.id, &pending.txid, "").is_err());
        let again = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "empty-hash",
                None,
            )
            .unwrap();
        assert_eq!(again.id, pending.id);
        assert_eq!(again.status, "broadcast");
        assert_eq!(again.block_hash, None);
    }

    #[test]
    fn rejects_reusing_a_key_for_a_different_payment() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();

        let error = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                13_000,
                "same",
                None,
            )
            .unwrap_err();

        assert!(error.to_string().contains("different payment"));
        assert!(error.downcast_ref::<IdempotencyConflict>().is_some());

        let error = store
            .claim_faucet(2, "ironwood", 12_000, "same")
            .unwrap_err();
        assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
    }

    #[test]
    fn reusing_a_key_conflicts_on_every_payment_field() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "send",
                None,
            )
            .unwrap();
        store.claim_faucet(2, "ironwood", 12_000, "faucet").unwrap();

        for (from, to, source, destination, amount) in [
            (3, 2, "ironwood", "ironwood", 12_000),
            (1, 3, "ironwood", "ironwood", 12_000),
            (1, 2, "transparent", "ironwood", 12_000),
            (1, 2, "ironwood", "transparent", 12_000),
            (1, 2, "ironwood", "ironwood", 13_000),
        ] {
            let error = store
                .claim_transfer(
                    from,
                    Recipient::Account(to),
                    source,
                    destination,
                    amount,
                    "send",
                    None,
                )
                .unwrap_err();
            assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
        }
        for (to, pool, amount) in [
            (3, "ironwood", 12_000),
            (2, "transparent", 12_000),
            (2, "ironwood", 13_000),
        ] {
            let error = store.claim_faucet(to, pool, amount, "faucet").unwrap_err();
            assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
        }
        let error = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "faucet",
                None,
            )
            .unwrap_err();
        assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
    }

    #[test]
    fn send_and_faucet_validate_their_own_requests() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();

        for (from, to, source, destination, amount) in [
            (9, 2, "ironwood", "ironwood", 12_000),
            (1, 9, "ironwood", "ironwood", 12_000),
            (1, 2, "sapling", "ironwood", 12_000),
            (1, 2, "ironwood", "sapling", 12_000),
            (1, 2, "ironwood", "ironwood", 0),
        ] {
            assert!(
                store
                    .claim_transfer(
                        from,
                        Recipient::Account(to),
                        source,
                        destination,
                        amount,
                        "send",
                        None
                    )
                    .is_err()
            );
        }
        for (to, pool, amount) in [
            (9, "ironwood", 12_000),
            (2, "sapling", 12_000),
            (2, "ironwood", 0),
        ] {
            assert!(store.claim_faucet(to, pool, amount, "faucet").is_err());
        }
        assert!(store.activity_for_key("send").unwrap().is_none());
        assert!(store.activity_for_key("faucet").unwrap().is_none());

        let faucet = store
            .claim_faucet(2, "transparent", 12_000, "faucet")
            .unwrap();
        assert_eq!(faucet.kind, "faucet");
        assert_eq!(faucet.from_account, None);
        assert_eq!(faucet.source_pool, "ironwood");
        assert_eq!(faucet.destination_pool, "transparent");
        assert_eq!(faucet.status, "preparing");
        assert!(faucet.txid.is_empty());
    }

    #[test]
    fn memo_is_part_of_the_claimed_payment() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let first = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "memo-key",
                Some("rent"),
            )
            .unwrap();
        assert_eq!(
            store
                .claim_transfer(
                    1,
                    Recipient::Account(2),
                    "ironwood",
                    "ironwood",
                    12_000,
                    "memo-key",
                    Some("rent")
                )
                .unwrap()
                .id,
            first.id
        );
        for memo in [None, Some(""), Some("gift")] {
            let error = store
                .claim_transfer(
                    1,
                    Recipient::Account(2),
                    "ironwood",
                    "ironwood",
                    12_000,
                    "memo-key",
                    memo,
                )
                .unwrap_err();
            assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
        }
    }

    #[test]
    fn existing_idempotency_tables_gain_a_memo_column() {
        let store = Store::open(":memory:").unwrap();
        store
            .0
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE idempotency (key TEXT PRIMARY KEY, activity_id TEXT NOT NULL)",
            )
            .unwrap();
        store.initialize().unwrap();
        store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "memo-key",
                Some("rent"),
            )
            .unwrap();
        assert_eq!(
            store
                .0
                .lock()
                .unwrap()
                .query_row(
                    "SELECT memo FROM idempotency WHERE key='memo-key'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "rent"
        );
    }

    #[test]
    fn concurrent_claims_create_one_payment() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let threads = (0..2)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .claim_transfer(
                            1,
                            Recipient::Account(2),
                            "ironwood",
                            "ironwood",
                            12_000,
                            "same",
                            None,
                        )
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let claims = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(claims[0].id, claims[1].id);
        assert_eq!(claims[0].status, "preparing");
        let listed = store.activities(10).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, claims[0].id);
        assert!(listed[0].txid.is_empty());
    }

    #[test]
    fn discarding_a_failed_prepare_removes_its_claim() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let failed = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();

        store.discard_preparing(&failed.id).unwrap();

        assert!(store.activity_for_key("same").unwrap().is_none());
        let retry = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        assert_ne!(retry.id, failed.id);
    }

    #[test]
    fn prepared_payment_can_be_reset_after_expiry() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        store
            .record_prepared(&claim.id, "expired-txid", b"signed transaction", 140)
            .unwrap();

        let retry = store.reset_for_retry(&claim.id, "expired-txid").unwrap();

        assert_eq!(retry.status, "preparing");
        assert!(retry.txid.is_empty());
        assert!(store.prepared_transaction(&claim.id).is_err());
        assert_eq!(store.activities(10).unwrap(), [retry]);
    }

    #[test]
    fn recording_the_same_prepared_transaction_converges() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        let first = store
            .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
            .unwrap();

        let second = store
            .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
            .unwrap();

        assert_eq!(second.id, first.id);
        assert_eq!(second.txid, first.txid);
        assert_eq!(second.status, "prepared");
    }

    #[test]
    fn recording_prepared_bytes_backfills_a_legacy_broadcast() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        let prepared = store
            .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
            .unwrap();
        let broadcast = store.mark_broadcast(&prepared.id, &prepared.txid).unwrap();
        store
            .0
            .lock()
            .unwrap()
            .execute("DELETE FROM prepared_payments", [])
            .unwrap();

        store
            .record_prepared(
                &broadcast.id,
                &broadcast.txid,
                b"recovered transaction",
                140,
            )
            .unwrap();

        assert_eq!(
            store
                .prepared_transaction(&broadcast.id)
                .unwrap()
                .raw_transaction,
            b"recovered transaction"
        );
    }

    #[test]
    fn stale_retry_does_not_reset_a_replacement_transaction() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        store
            .record_prepared(&claim.id, "old-txid", b"old transaction", 140)
            .unwrap();
        store.reset_for_retry(&claim.id, "old-txid").unwrap();
        store
            .record_prepared(&claim.id, "new-txid", b"new transaction", 180)
            .unwrap();

        let current = store.reset_for_retry(&claim.id, "old-txid").unwrap();

        assert_eq!(current.txid, "new-txid");
        assert_eq!(current.status, "prepared");
    }

    #[test]
    fn broadcast_reconciliation_preserves_a_concurrent_confirmation() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        store
            .record_prepared(&claim.id, "real-txid", b"transaction", 140)
            .unwrap();
        store
            .confirm(&claim.id, "real-txid", "mined-block")
            .unwrap();

        let current = store.mark_broadcast(&claim.id, "real-txid").unwrap();

        assert_eq!(current.status, "confirmed");
        assert_eq!(current.block_hash.as_deref(), Some("mined-block"));
    }

    #[test]
    fn prepared_transaction_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();
        let id = claim.id.clone();
        store
            .record_prepared(&id, "real-txid", b"raw transaction", 140)
            .unwrap();
        drop(store);

        let reopened = Store::open(path).unwrap();
        reopened.initialize().unwrap();
        let recovered = reopened
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same",
                None,
            )
            .unwrap();

        assert_eq!(recovered.txid, "real-txid");
        assert_eq!(recovered.status, "prepared");
        let prepared = reopened.prepared_transaction(&id).unwrap();
        assert_eq!(prepared.raw_transaction, b"raw transaction");
        assert_eq!(prepared.expiry_height, 140);
    }

    #[test]
    fn records_a_send_to_an_external_address() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let activity = store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                12_000,
                "external-key",
                None,
            )
            .unwrap();
        assert_eq!(activity.to_account, None);
        assert_eq!(activity.to_address.as_deref(), Some("tmExternal"));

        let replayed = store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                12_000,
                "external-key",
                None,
            )
            .unwrap();
        assert_eq!(replayed.id, activity.id);

        // Reusing the key for a different recipient is a different payment.
        let conflict = store
            .claim_transfer(
                1,
                Recipient::Address("tmOther"),
                "ironwood",
                "transparent",
                12_000,
                "external-key",
                None,
            )
            .unwrap_err();
        assert!(conflict.downcast_ref::<IdempotencyConflict>().is_some());
    }

    #[test]
    fn external_recipient_survives_recovery_and_shares_the_faucet_key_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let claimed = store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                12_000,
                "external-send",
                None,
            )
            .unwrap();
        store
            .record_prepared(&claimed.id, "txid-external", b"signed transaction", 140)
            .unwrap();
        drop(store);

        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let replayed = store.activity_for_key("external-send").unwrap().unwrap();
        assert_eq!(replayed.id, claimed.id);
        assert_eq!(replayed.to_account, None);
        assert_eq!(replayed.to_address.as_deref(), Some("tmExternal"));
        assert_eq!(store.unconfirmed_activities().unwrap()[0].id, claimed.id);
        assert_eq!(
            store
                .prepared_transaction(&claimed.id)
                .unwrap()
                .raw_transaction,
            b"signed transaction"
        );
        let broadcast = store.mark_broadcast(&claimed.id, "txid-external").unwrap();
        assert_eq!(broadcast.to_address.as_deref(), Some("tmExternal"));
        let confirmed = store
            .confirm(&claimed.id, "txid-external", "block")
            .unwrap();
        assert_eq!(confirmed.to_address.as_deref(), Some("tmExternal"));
        assert_eq!(store.activities(10).unwrap()[0].to_account, None);
        assert!(store.unconfirmed_activities().unwrap().is_empty());

        let conflict = store
            .claim_address_faucet("tmExternal", 12_000, "external-send")
            .unwrap_err();
        assert!(conflict.downcast_ref::<IdempotencyConflict>().is_some());
        store
            .claim_address_faucet("tmExternal", 12_000, "external-faucet")
            .unwrap();
        let conflict = store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                12_000,
                "external-faucet",
                None,
            )
            .unwrap_err();
        assert!(conflict.downcast_ref::<IdempotencyConflict>().is_some());
    }

    #[test]
    fn migrates_activity_created_before_address_sends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        {
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE activity (
                     id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_account INTEGER,
                     to_account INTEGER NOT NULL, source_pool TEXT NOT NULL,
                     destination_pool TEXT NOT NULL, amount_zatoshi INTEGER NOT NULL,
                     txid TEXT NOT NULL, block_hash TEXT, status TEXT NOT NULL,
                     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 INSERT INTO activity(id,kind,from_account,to_account,source_pool,destination_pool,amount_zatoshi,txid,status)
                 VALUES('older','send',1,2,'ironwood','ironwood',1,'txid-older','confirmed'),
                       ('newer','send',1,3,'ironwood','ironwood',2,'txid-newer','broadcast');",
            )
            .unwrap();
        }

        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let activity = store.activities(10).unwrap();
        assert_eq!(
            activity.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
            ["newer", "older"]
        );
        assert_eq!(activity[0].to_account, Some(3));
        assert_eq!(activity[0].to_address, None);

        store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                3,
                "after-migration",
                None,
            )
            .unwrap();
        // Initializing again must keep rows written after the migration.
        store.initialize().unwrap();
        let claimed = store.activity_for_key("after-migration").unwrap().unwrap();
        assert_eq!(claimed.to_address.as_deref(), Some("tmExternal"));
        assert_eq!(store.activities(10).unwrap().len(), 3);
    }
    #[test]
    fn older_stores_gain_payment_errors_without_losing_claims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        {
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE activity (
                     id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_account INTEGER,
                     to_account INTEGER NOT NULL, source_pool TEXT NOT NULL,
                     destination_pool TEXT NOT NULL, amount_zatoshi INTEGER NOT NULL,
                     txid TEXT NOT NULL, block_hash TEXT, status TEXT NOT NULL,
                     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 CREATE TABLE idempotency (key TEXT PRIMARY KEY, activity_id TEXT NOT NULL);
                 CREATE TABLE prepared_payments (
                     activity_id TEXT PRIMARY KEY, raw_transaction BLOB NOT NULL,
                     expiry_height INTEGER NOT NULL
                 );
                 CREATE TABLE address_faucets (
                     key TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, address TEXT NOT NULL,
                     amount_zatoshi INTEGER NOT NULL, txid TEXT NOT NULL DEFAULT '',
                     block_hash TEXT, status TEXT NOT NULL DEFAULT 'preparing'
                 );
                 INSERT INTO activity(id,kind,from_account,to_account,source_pool,destination_pool,amount_zatoshi,txid,status)
                     VALUES('legacy-send','send',1,2,'ironwood','ironwood',12000,'legacy-txid','broadcast');
                 INSERT INTO idempotency(key,activity_id) VALUES('legacy-key','legacy-send');
                 INSERT INTO prepared_payments VALUES('legacy-send',x'0102',140);
                 INSERT INTO address_faucets(key,id,address,amount_zatoshi) VALUES('legacy-address','legacy-faucet','tm-external',7);",
            )
            .unwrap();
        }

        for _ in 0..2 {
            let store = Store::open(&path).unwrap();
            store.initialize().unwrap();
            let send = store
                .claim_transfer(
                    1,
                    crate::db::Recipient::Account(2),
                    "ironwood",
                    "ironwood",
                    12_000,
                    "legacy-key",
                    None,
                )
                .unwrap();
            assert_eq!(
                (send.id.as_str(), send.txid.as_str(), send.status.as_str()),
                ("legacy-send", "legacy-txid", "broadcast")
            );
            assert_eq!(send.last_error, None);
            assert_eq!(
                store.prepared_transaction(&send.id).unwrap(),
                PreparedTransaction {
                    raw_transaction: vec![1, 2],
                    expiry_height: 140
                }
            );
            let faucet = store
                .claim_address_faucet("tm-external", 7, "legacy-address")
                .unwrap();
            assert_eq!(faucet.id, "legacy-faucet");
            assert_eq!(faucet.last_error, None);
        }

        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        store
            .record_error(
                "legacy-send",
                "broadcast",
                "legacy-txid",
                "lookup unavailable",
            )
            .unwrap();
        store
            .fail_address("legacy-faucet", "treasury cannot pay")
            .unwrap();
        drop(store);
        let reopened = Store::open(&path).unwrap();
        reopened.initialize().unwrap();
        assert_eq!(
            reopened
                .activity_for_key("legacy-key")
                .unwrap()
                .unwrap()
                .last_error
                .as_deref(),
            Some("lookup unavailable")
        );
        let failed = reopened
            .address_faucet_for_key("legacy-address")
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.last_error.as_deref(), Some("treasury cannot pay"));
    }

    #[test]
    fn unresolved_intents_include_unprepared_claims_and_exclude_terminal_records() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let absent = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                1_000,
                "absent-memo",
                None,
            )
            .unwrap();
        let empty = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                2_000,
                "empty-memo",
                Some(""),
            )
            .unwrap();
        let text = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "transparent",
                "ironwood",
                3_000,
                "text-memo",
                Some("rent"),
            )
            .unwrap();
        let faucet = store
            .claim_faucet(3, "transparent", 4_000, "faucet")
            .unwrap();
        let broadcast = store
            .claim_transfer(
                2,
                crate::db::Recipient::Account(1),
                "ironwood",
                "transparent",
                5_000,
                "broadcast",
                None,
            )
            .unwrap();
        store
            .record_prepared(&broadcast.id, "broadcast-txid", b"raw", 140)
            .unwrap();
        let broadcast = store
            .mark_broadcast(&broadcast.id, "broadcast-txid")
            .unwrap();
        let confirmed = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(3),
                "ironwood",
                "ironwood",
                6_000,
                "confirmed",
                None,
            )
            .unwrap();
        store
            .record_prepared(&confirmed.id, "confirmed-txid", b"raw", 140)
            .unwrap();
        store
            .confirm(&confirmed.id, "confirmed-txid", "block")
            .unwrap();
        let failed = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(4),
                "ironwood",
                "ironwood",
                7_000,
                "failed",
                None,
            )
            .unwrap();
        store.fail(&failed.id, "insufficient funds").unwrap();

        let intents = store.unresolved_activities().unwrap();
        let summary: Vec<_> = intents
            .iter()
            .map(|intent| {
                (
                    intent.activity.id.as_str(),
                    intent.key.as_str(),
                    intent.memo.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (absent.id.as_str(), "absent-memo", None),
                (empty.id.as_str(), "empty-memo", Some("")),
                (text.id.as_str(), "text-memo", Some("rent")),
                (faucet.id.as_str(), "faucet", None),
                (broadcast.id.as_str(), "broadcast", None),
            ]
        );
        let text = &intents[2].activity;
        assert_eq!(
            (
                text.kind.as_str(),
                text.from_account,
                text.to_account,
                text.source_pool.as_str(),
                text.destination_pool.as_str(),
                text.amount_zatoshi,
                text.txid.as_str(),
                text.status.as_str(),
            ),
            (
                "send",
                Some(1),
                Some(2),
                "transparent",
                "ironwood",
                3_000,
                "",
                "preparing"
            )
        );
        assert_eq!(intents[3].activity.kind, "faucet");
        assert_eq!(intents[3].activity.from_account, None);
        assert_eq!(intents[3].activity.destination_pool, "transparent");
        assert_eq!(intents[4].activity, broadcast);
        assert_eq!(
            store
                .unconfirmed_activities()
                .unwrap()
                .iter()
                .map(|activity| activity.id.as_str())
                .collect::<Vec<_>>(),
            [broadcast.id.as_str()]
        );

        let preparing = store
            .claim_address_faucet("tm-preparing", 10, "address-preparing")
            .unwrap();
        let prepared = store
            .claim_address_faucet("u-prepared", 20, "address-prepared")
            .unwrap();
        let prepared = store
            .record_address_prepared(&prepared.id, "address-txid", b"raw", 140)
            .unwrap();
        let confirmed = store
            .claim_address_faucet("tm-confirmed", 30, "address-confirmed")
            .unwrap();
        store
            .record_address_prepared(&confirmed.id, "confirmed-address-txid", b"raw", 140)
            .unwrap();
        store
            .confirm_address(&confirmed.id, "confirmed-address-txid", "block")
            .unwrap();
        let failed = store
            .claim_address_faucet("tm-failed", 40, "address-failed")
            .unwrap();
        store
            .fail_address(&failed.id, "treasury cannot pay")
            .unwrap();

        assert_eq!(
            store.unresolved_address_faucets().unwrap(),
            [
                AddressFaucetIntent {
                    payment: preparing,
                    key: "address-preparing".into(),
                },
                AddressFaucetIntent {
                    payment: prepared,
                    key: "address-prepared".into(),
                },
            ]
        );
    }

    #[test]
    fn payment_errors_and_failures_never_overwrite_newer_state() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "guarded",
                None,
            )
            .unwrap();
        let errored = store
            .record_error(&claim.id, "preparing", "", "wallet is syncing")
            .unwrap();
        assert_eq!(errored.last_error.as_deref(), Some("wallet is syncing"));
        let prepared = store
            .record_prepared(&claim.id, "txid-1", b"raw", 140)
            .unwrap();
        assert_eq!(prepared.last_error, None);

        let stale = store
            .record_error(&claim.id, "preparing", "", "stale preparation error")
            .unwrap();
        assert_eq!(stale.status, "prepared");
        assert_eq!(stale.last_error, None);
        assert!(store.fail(&claim.id, "stale insufficient funds").is_err());
        let current = store.activity_for_key("guarded").unwrap().unwrap();
        assert_eq!(
            (
                current.status.as_str(),
                current.txid.as_str(),
                current.last_error
            ),
            ("prepared", "txid-1", None)
        );

        store
            .record_error(&claim.id, "prepared", "txid-1", "broadcast unavailable")
            .unwrap();
        let broadcast = store.mark_broadcast(&claim.id, "txid-1").unwrap();
        assert_eq!(broadcast.last_error, None);
        store
            .record_error(&claim.id, "broadcast", "txid-1", "auto-mine failed")
            .unwrap();
        let confirmed = store.confirm(&claim.id, "txid-1", "block").unwrap();
        assert_eq!(confirmed.last_error, None);
        let after = store
            .record_error(&claim.id, "confirmed", "txid-1", "late error")
            .unwrap();
        assert_eq!(after.last_error, None);
        assert!(store.fail(&claim.id, "late failure").is_err());
        assert_eq!(
            store.activity_for_key("guarded").unwrap().unwrap().status,
            "confirmed"
        );

        let doomed = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                99_000,
                "doomed",
                Some(""),
            )
            .unwrap();
        let failed = store.fail(&doomed.id, "insufficient funds").unwrap();
        assert_eq!(
            (failed.status.as_str(), failed.txid.as_str()),
            ("failed", "")
        );
        assert_eq!(failed.last_error.as_deref(), Some("insufficient funds"));
        let again = store.fail(&doomed.id, "a different reason").unwrap();
        assert_eq!(again.last_error.as_deref(), Some("insufficient funds"));
        store
            .record_error(&doomed.id, "failed", "", "overwrite")
            .unwrap();
        assert!(
            store
                .record_prepared(&doomed.id, "late-txid", b"late", 140)
                .is_err()
        );
        assert!(store.mark_broadcast(&doomed.id, "late-txid").is_err());
        store.reset_for_retry(&doomed.id, "").unwrap();
        store.discard_preparing(&doomed.id).unwrap();
        let replay = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                99_000,
                "doomed",
                Some(""),
            )
            .unwrap();
        assert_eq!(replay.id, doomed.id);
        assert_eq!(replay.status, "failed");
        assert_eq!(replay.last_error.as_deref(), Some("insufficient funds"));
        assert!(store.prepared_transaction(&doomed.id).is_err());
        let conflict = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                99_000,
                "doomed",
                None,
            )
            .unwrap_err();
        assert!(conflict.downcast_ref::<IdempotencyConflict>().is_some());
    }

    #[test]
    fn address_faucet_errors_and_failures_never_overwrite_newer_state() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_address_faucet("tm-external", 100, "address-guarded")
            .unwrap();
        store
            .record_address_error(&claim.id, "preparing", "", "treasury is syncing")
            .unwrap();
        let prepared = store
            .record_address_prepared(&claim.id, "txid-1", b"raw", 140)
            .unwrap();
        assert_eq!(prepared.last_error, None);
        let stale = store
            .record_address_error(&claim.id, "preparing", "", "stale")
            .unwrap();
        assert_eq!(stale.last_error, None);
        assert!(store.fail_address(&claim.id, "stale").is_err());
        store
            .record_address_error(&claim.id, "prepared", "txid-1", "broadcast unavailable")
            .unwrap();
        assert_eq!(
            store
                .mark_address_broadcast(&claim.id, "txid-1")
                .unwrap()
                .last_error,
            None
        );
        store
            .record_address_error(&claim.id, "broadcast", "txid-1", "auto-mine failed")
            .unwrap();
        let confirmed = store.confirm_address(&claim.id, "txid-1", "block").unwrap();
        assert_eq!(confirmed.last_error, None);
        assert_eq!(
            store
                .record_address_error(&claim.id, "confirmed", "txid-1", "late")
                .unwrap()
                .last_error,
            None
        );
        assert!(store.fail_address(&claim.id, "late").is_err());

        let doomed = store
            .claim_address_faucet("tm-external", 100, "address-doomed")
            .unwrap();
        let failed = store
            .fail_address(&doomed.id, "treasury cannot pay")
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(
            store
                .fail_address(&doomed.id, "another reason")
                .unwrap()
                .last_error
                .as_deref(),
            Some("treasury cannot pay")
        );
        store
            .record_address_error(&doomed.id, "failed", "", "overwrite")
            .unwrap();
        assert!(
            store
                .record_address_prepared(&doomed.id, "late-txid", b"late", 140)
                .is_err()
        );
        store.discard_address_preparing(&doomed.id).unwrap();
        let replay = store
            .claim_address_faucet("tm-external", 100, "address-doomed")
            .unwrap();
        assert_eq!(replay, failed);
    }

    #[test]
    fn repaired_preparation_never_replaces_a_newer_or_terminal_transaction() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "repair",
                None,
            )
            .unwrap();
        store
            .record_prepared(&claim.id, "old-txid", b"old", 140)
            .unwrap();
        store.reset_for_retry(&claim.id, "old-txid").unwrap();
        store
            .record_prepared(&claim.id, "new-txid", b"new", 180)
            .unwrap();

        assert!(
            store
                .record_prepared(&claim.id, "old-txid", b"old", 140)
                .is_err()
        );
        let current = store.activity_for_key("repair").unwrap().unwrap();
        assert_eq!(current.txid, "new-txid");
        assert_eq!(
            store.prepared_transaction(&claim.id).unwrap(),
            PreparedTransaction {
                raw_transaction: b"new".to_vec(),
                expiry_height: 180
            }
        );
        store
            .record_prepared(&claim.id, "new-txid", b"different bytes", 999)
            .unwrap();
        assert_eq!(
            store
                .prepared_transaction(&claim.id)
                .unwrap()
                .raw_transaction,
            b"new"
        );
        store.confirm(&claim.id, "new-txid", "block").unwrap();
        assert!(
            store
                .record_prepared(&claim.id, "other-txid", b"other", 200)
                .is_err()
        );
        let confirmed = store.activity_for_key("repair").unwrap().unwrap();
        assert_eq!(
            (confirmed.status.as_str(), confirmed.txid.as_str()),
            ("confirmed", "new-txid")
        );

        let address = store
            .claim_address_faucet("tm-external", 5, "address-repair")
            .unwrap();
        store
            .record_address_prepared(&address.id, "address-new", b"new", 180)
            .unwrap();
        assert!(
            store
                .record_address_prepared(&address.id, "address-old", b"old", 140)
                .is_err()
        );
        store
            .confirm_address(&address.id, "address-new", "block")
            .unwrap();
        assert!(
            store
                .record_address_prepared(&address.id, "address-other", b"other", 200)
                .is_err()
        );
        let current = store
            .address_faucet_for_key("address-repair")
            .unwrap()
            .unwrap();
        assert_eq!(
            (current.status.as_str(), current.txid.as_str()),
            ("confirmed", "address-new")
        );
        assert_eq!(
            store
                .prepared_transaction(&address.id)
                .unwrap()
                .raw_transaction,
            b"new"
        );
    }
    #[test]
    fn recipient_and_status_upgrades_preserve_errors_and_signed_payments_in_both_orders() {
        for recipients_first in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("server.db");
            let db = Connection::open(&path).unwrap();
            let recipients = if recipients_first {
                "to_account INTEGER, to_address TEXT,"
            } else {
                "to_account INTEGER NOT NULL,"
            };
            let error = if recipients_first {
                ""
            } else {
                ", last_error TEXT"
            };
            db.execute_batch(&format!("CREATE TABLE activity (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, from_account INTEGER,
                {recipients} source_pool TEXT NOT NULL, destination_pool TEXT NOT NULL,
                amount_zatoshi INTEGER NOT NULL, txid TEXT NOT NULL, block_hash TEXT,
                status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP {error});
                CREATE TABLE idempotency(key TEXT PRIMARY KEY, activity_id TEXT NOT NULL, memo TEXT);
                CREATE TABLE prepared_payments(activity_id TEXT PRIMARY KEY, raw_transaction BLOB NOT NULL, expiry_height INTEGER NOT NULL);
                INSERT INTO activity(id,kind,from_account,to_account,source_pool,destination_pool,amount_zatoshi,txid,status)
                    VALUES('failed','send',1,2,'ironwood','ironwood',12000,'','failed'),
                          ('signed','send',1,3,'ironwood','ironwood',13000,'original-txid','broadcast');
                INSERT INTO idempotency VALUES('failed-key','failed',''),('signed-key','signed','memo');
                INSERT INTO prepared_payments VALUES('signed',x'010203',140);" )).unwrap();
            db.execute("UPDATE activity SET created_at='2025-01-02 03:04:05'", [])
                .unwrap();
            if recipients_first {
                db.execute_batch("INSERT INTO activity(id,kind,from_account,to_account,to_address,source_pool,destination_pool,amount_zatoshi,txid,status,created_at)
                    VALUES('external','send',2,NULL,'tmExternal','ironwood','transparent',14000,'','preparing','2025-02-03 04:05:06');
                    INSERT INTO idempotency VALUES('external-key','external','');").unwrap();
            }
            if !recipients_first {
                db.execute(
                    "UPDATE activity SET last_error='original failure' WHERE id='failed'",
                    [],
                )
                .unwrap();
            }
            drop(db);
            for _ in 0..2 {
                let store = Store::open(&path).unwrap();
                store.initialize().unwrap();
                let failed = store.activity_for_key("failed-key").unwrap().unwrap();
                assert_eq!(failed.id, "failed");
                assert_eq!(failed.status, "failed");
                assert_eq!(failed.created_at, "2025-01-02 03:04:05");
                assert_eq!(failed.to_account, Some(2));
                assert_eq!(failed.to_address, None);
                assert_eq!(
                    failed.last_error.as_deref(),
                    if recipients_first {
                        None
                    } else {
                        Some("original failure")
                    }
                );
                let intents = store.unresolved_activities().unwrap();
                assert_eq!(intents.len(), if recipients_first { 2 } else { 1 });
                assert_eq!(intents[0].activity.id, "signed");
                assert_eq!(intents[0].activity.status, "broadcast");
                assert_eq!(intents[0].activity.created_at, "2025-01-02 03:04:05");
                assert_eq!(intents[0].key, "signed-key");
                if recipients_first {
                    let external = &intents[1];
                    assert_eq!(external.activity.id, "external");
                    assert_eq!(external.activity.to_account, None);
                    assert_eq!(external.activity.to_address.as_deref(), Some("tmExternal"));
                    assert_eq!(external.activity.created_at, "2025-02-03 04:05:06");
                    assert_eq!(external.key, "external-key");
                    assert_eq!(external.memo.as_deref(), Some(""));
                    assert_eq!(external.activity.status, "preparing");
                    assert_eq!(external.activity.txid, "");
                    assert_eq!(external.activity.last_error, None);
                }
                assert_eq!(intents[0].memo.as_deref(), Some("memo"));
                assert_eq!(intents[0].activity.txid, "original-txid");
                assert_eq!(
                    store.prepared_transaction("signed").unwrap(),
                    PreparedTransaction {
                        raw_transaction: vec![1, 2, 3],
                        expiry_height: 140
                    }
                );
            }
        }
    }

    #[test]
    fn unresolved_external_sends_preserve_recipient_key_and_memo_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        store
            .claim_transfer(
                1,
                Recipient::Address("tmExternal"),
                "ironwood",
                "transparent",
                12000,
                "external-intent",
                Some(""),
            )
            .unwrap();
        let activity = store.activity_for_key("external-intent").unwrap().unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        assert_eq!(
            store.unresolved_activities().unwrap(),
            vec![ActivityIntent {
                activity,
                key: "external-intent".into(),
                memo: Some("".into())
            }]
        );
    }
}
