use std::{
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    handler::HandlerWithoutStateExt,
    http::{HeaderMap, StatusCode, Uri, header::HOST},
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, RwLock, broadcast},
    time::Instant,
};
use tower_http::{services::ServeDir, trace::TraceLayer};
use zcash_keys::{address::Address, encoding::AddressCodec};
use zcash_protocol::{
    consensus::COINBASE_MATURITY_BLOCKS,
    memo::{Memo, MemoBytes},
    value::{MAX_MONEY, Zatoshis},
};
use zip321::{Payment, TransactionRequest};

use crate::{
    db::{
        Account, Activity, AddressFaucet, IdempotencyConflict, PreparedTransaction, Recipient,
        Store, TREASURY_ACCOUNT_ID, USER_ACCOUNT_COUNT, ZATOSHIS_PER_ZEC,
    },
    mining::{
        Admission, MiningAdmissionError, MiningCoordinator, MiningJob, MiningRuntime, MiningState,
        validate_idempotency_key,
    },
    rpc::{ChainCheckpoint, ChainInfo, NodeRpc, RpcError},
    wallet::{
        PaymentError, PreparedPayment, RealWallet, SendQuote, WALLET_BIRTHDAY_HEIGHT,
        regtest_network,
    },
};

#[derive(Clone)]
pub struct AppState(Arc<Inner>);
struct Inner {
    mining: Arc<MiningCoordinator>,
    #[cfg(test)]
    mining_runtime: Option<Arc<dyn MiningRuntime>>,
    store: Store,
    wallet: RealWallet,
    rpc: NodeRpc,
    instance: String,
    events: broadcast::Sender<String>,
    payments: Mutex<()>,
    wallet_sync: Mutex<()>,
    treasury_replenishment: Mutex<()>,
    wallet_snapshot: RwLock<WalletSnapshot>,
    started: AtomicBool,
}

#[derive(Clone)]
struct WalletSnapshot {
    accounts: Vec<Account>,
    status: WalletSyncStatus,
}

#[derive(Clone, Serialize)]
struct WalletSyncStatus {
    state: &'static str,
    fully_scanned_height: Option<u64>,
    fully_scanned_hash: Option<String>,
    observed_height: Option<u64>,
    observed_hash: Option<String>,
    last_success_at: Option<u64>,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
struct SettlementStatus {
    node_height: Option<u64>,
    lightwalletd_height: Option<u64>,
    wallet_scanned_height: Option<u64>,
    wallet_sync_state: &'static str,
    settled: bool,
    settlement_blocker: Option<&'static str>,
}

fn settlement_status(
    node: Option<&ChainCheckpoint>,
    lightwalletd: Option<&ChainCheckpoint>,
    wallet: &WalletSyncStatus,
) -> SettlementStatus {
    let settlement_blocker = if node.is_none() {
        Some("node_unavailable")
    } else if lightwalletd.is_none() {
        Some("lightwalletd_unavailable")
    } else if wallet.state == "error" {
        Some("wallet_error")
    } else if wallet.state != "ready" {
        Some("wallet_syncing")
    } else if node.map(|tip| tip.height) != lightwalletd.map(|tip| tip.height) {
        Some("height_mismatch")
    } else if node != lightwalletd {
        Some("chain_mismatch")
    } else if !node.is_some_and(|tip| wallet_matches_tip(wallet, tip)) {
        Some("wallet_behind")
    } else {
        None
    };

    SettlementStatus {
        node_height: node.map(|tip| tip.height),
        lightwalletd_height: lightwalletd.map(|tip| tip.height),
        wallet_scanned_height: wallet.fully_scanned_height,
        wallet_sync_state: wallet.state,
        settled: settlement_blocker.is_none(),
        settlement_blocker,
    }
}

fn wallet_matches_tip(wallet: &WalletSyncStatus, tip: &ChainCheckpoint) -> bool {
    wallet.state == "ready"
        && ((wallet.fully_scanned_height == Some(tip.height)
            && wallet.fully_scanned_hash.as_deref() == Some(&tip.hash))
            || (tip.height < u64::from(WALLET_BIRTHDAY_HEIGHT)
                && wallet.fully_scanned_height.is_none()
                && wallet.observed_height == Some(tip.height)
                && wallet.observed_hash.as_deref() == Some(&tip.hash)))
}

impl AppState {
    pub fn new(store: Store, wallet: RealWallet, rpc: String, instance: String) -> Self {
        let (events, _) = broadcast::channel(128);
        let accounts = store.accounts().unwrap_or_default();
        Self(Arc::new(Inner {
            mining: Arc::new(MiningCoordinator::new()),
            #[cfg(test)]
            mining_runtime: None,
            store,
            wallet,
            rpc: NodeRpc::new(rpc),
            instance,
            events,
            payments: Mutex::new(()),
            wallet_sync: Mutex::new(()),
            treasury_replenishment: Mutex::new(()),
            wallet_snapshot: RwLock::new(WalletSnapshot {
                accounts,
                status: WalletSyncStatus {
                    state: "syncing",
                    fully_scanned_height: None,
                    fully_scanned_hash: None,
                    observed_height: None,
                    observed_hash: None,
                    last_success_at: None,
                    error: None,
                },
            }),
            started: AtomicBool::new(false),
        }))
    }

    pub fn mark_started(&self) {
        self.0.started.store(true, Ordering::Release);
    }

    fn started(&self) -> bool {
        self.0.started.load(Ordering::Acquire)
    }

    async fn accounts(&self) -> Vec<Account> {
        self.0.wallet_snapshot.read().await.accounts.clone()
    }

    async fn wallet_sync_status(&self) -> WalletSyncStatus {
        self.0.wallet_snapshot.read().await.status.clone()
    }

    async fn synchronize_wallet(&self, target_height: Option<u64>) -> anyhow::Result<()> {
        self.synchronize_wallet_until(target_height, Instant::now() + Duration::from_secs(120))
            .await
    }

    async fn synchronize_wallet_until(
        &self,
        target_height: Option<u64>,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let _guard = tokio::time::timeout_at(deadline, self.0.wallet_sync.lock()).await?;

        {
            let mut snapshot = self.0.wallet_snapshot.write().await;
            snapshot.status.state = "syncing";
            snapshot.status.error = None;
        }
        notify(self, "sync");

        let result = async {
            if let Some(target) = target_height {
                self.0
                    .wallet
                    .wait_for_height(target, deadline.saturating_duration_since(Instant::now()))
                    .await?;
            }
            let target = crate::reconcile::within_deadline(
                deadline,
                "reading the reconciliation target",
                self.0.rpc.checkpoint(),
            )
            .await?;
            if let Some(height) = target_height {
                anyhow::ensure!(
                    target.height >= height,
                    "mined height is no longer canonical"
                );
            }
            crate::reconcile::sync_wallet(&self.0.wallet, &self.0.rpc, &target, deadline).await?;
            self.refresh_wallet_snapshot(&target).await
        }
        .await;

        if let Err(error) = &result {
            let mut snapshot = self.0.wallet_snapshot.write().await;
            snapshot.status.state = "error";
            snapshot.status.error = Some(error.to_string());
            notify(self, "sync");
        }
        result
    }

    async fn synchronize_latest(&self) -> anyhow::Result<()> {
        self.synchronize_wallet(None).await
    }

    async fn refresh_wallet_snapshot(&self, target: &ChainCheckpoint) -> anyhow::Result<()> {
        let mut accounts = self.0.store.accounts()?;
        self.0.wallet.apply_balances(&mut accounts).await?;
        let scanned = self.0.wallet.scanned_checkpoint().await?;
        let rpc = &self.0.rpc;
        let observed = rpc.checkpoint().await?;
        let publishable = scan_is_publishable(scanned.as_ref(), target, &observed, |height| {
            rpc.block_hash(height)
        })
        .await?;
        if !publishable {
            return Err(crate::ReadinessError::CheckpointChanged.into());
        }
        let changed = self.0.wallet_snapshot.read().await.accounts != accounts;
        {
            let mut snapshot = self.0.wallet_snapshot.write().await;
            snapshot.accounts = accounts;
            snapshot.status = WalletSyncStatus {
                state: "ready",
                fully_scanned_height: scanned.as_ref().map(|checkpoint| checkpoint.height),
                fully_scanned_hash: scanned.as_ref().map(|checkpoint| checkpoint.hash.clone()),
                observed_height: Some(observed.height),
                observed_hash: Some(observed.hash),
                last_success_at: Some(now_unix()),
                error: None,
            };
        }
        notify(self, "sync");
        if changed {
            notify(self, "wallet");
        }
        Ok(())
    }

    async fn sync_if_chain_advanced(&self) -> anyhow::Result<()> {
        let observed = self.0.rpc.checkpoint().await?;
        let status = self.0.wallet_snapshot.read().await.status.clone();
        let settled = wallet_matches_tip(&status, &observed);
        {
            let mut snapshot = self.0.wallet_snapshot.write().await;
            snapshot.status.observed_height = Some(observed.height);
            snapshot.status.observed_hash = Some(observed.hash.clone());
        }
        if !settled {
            self.synchronize_wallet(Some(observed.height)).await?;
            notify(self, "chain");
        }
        Ok(())
    }
}

/// a scan that reached its target stays publishable after other requests mine past it, as long as
/// the node still has the scanned block.
async fn scan_is_publishable<F, Fut>(
    scanned: Option<&ChainCheckpoint>,
    target: &ChainCheckpoint,
    observed: &ChainCheckpoint,
    canonical_hash: F,
) -> anyhow::Result<bool>
where
    F: FnOnce(u32) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<String>>,
{
    Ok(match scanned {
        Some(scanned) if scanned == observed => scanned.height >= target.height,
        Some(scanned) if (target.height..observed.height).contains(&scanned.height) => {
            canonical_hash(u32::try_from(scanned.height)?).await? == scanned.hash
        }
        Some(_) => false,
        None => observed.height < u64::from(WALLET_BIRTHDAY_HEIGHT),
    })
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub async fn wallet_sync_loop(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = state.sync_if_chain_advanced().await {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                "background wallet synchronization failed"
            );
            let mut snapshot = state.0.wallet_snapshot.write().await;
            snapshot.status.state = "error";
            snapshot.status.error = Some(error.to_string());
            drop(snapshot);
            notify(&state, "sync");
        }
    }
}

const PAYMENT_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const PAYMENT_DISPATCH_BUDGET: Duration = Duration::from_secs(10);
const PAYMENT_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

async fn bounded_payment_chain_info(state: &AppState) -> anyhow::Result<ChainInfo> {
    bounded_payment_chain_info_until(state, Instant::now() + PAYMENT_LOOKUP_TIMEOUT).await
}

async fn bounded_payment_chain_info_until(
    state: &AppState,
    deadline: Instant,
) -> anyhow::Result<ChainInfo> {
    tokio::time::timeout_at(deadline, state.0.rpc.chain_info())
        .await
        .context("payment progression chain lookup timed out")?
}

async fn bounded_payment_transaction_known(state: &AppState, txid: &str) -> anyhow::Result<bool> {
    tokio::time::timeout(PAYMENT_LOOKUP_TIMEOUT, state.0.rpc.transaction_known(txid))
        .await
        .context("payment progression transaction lookup timed out")?
}

pub async fn payment_confirmation_loop(state: AppState) {
    let mut cursor = None;
    let mut activity_cursor = None;
    let mut address_cursor = None;
    let mut interval = tokio::time::interval(PAYMENT_RETRY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let deadline = Instant::now() + PAYMENT_DISPATCH_BUDGET;
        if state.started()
            && let Err(error) = progress_payment_intents(
                &state,
                deadline,
                &mut activity_cursor,
                &mut address_cursor,
            )
            .await
        {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                "background payment progression failed"
            );
        }
        let rpc = &state.0.rpc;
        match reconcile_unconfirmed_with(&state.0.store, &mut cursor, deadline, |txid| async move {
            rpc.lookup_transaction(&txid).await
        })
        .await
        {
            Ok(true) => notify(&state, "wallet"),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                error = %format_args!("{error:#}"),
                "background payment confirmation failed"
            ),
        }
    }
}

async fn progress_payment_intents(
    state: &AppState,
    deadline: Instant,
    activity_cursor: &mut Option<String>,
    address_cursor: &mut Option<String>,
) -> anyhow::Result<()> {
    let mut activities = state.0.store.unresolved_activities()?;
    activities.sort_by(|left, right| left.activity.id.cmp(&right.activity.id));
    let mut address_faucets = state.0.store.unresolved_address_faucets()?;
    address_faucets.sort_by(|left, right| left.payment.id.cmp(&right.payment.id));
    if activities.is_empty() {
        *activity_cursor = None;
    }
    if address_faucets.is_empty() {
        *address_cursor = None;
    }
    let activity_start = activity_cursor.as_ref().map_or(0, |id| {
        activities.partition_point(|intent| intent.activity.id.as_str() <= id.as_str())
            % activities.len().max(1)
    });
    let address_start = address_cursor.as_ref().map_or(0, |id| {
        address_faucets.partition_point(|intent| intent.payment.id.as_str() <= id.as_str())
            % address_faucets.len().max(1)
    });

    // Alternate between the internal and external queues. A slow internal
    // lookup must not consume the whole dispatch budget before external
    // faucets get a turn.
    for offset in 0..activities.len().max(address_faucets.len()) {
        if Instant::now() >= deadline {
            break;
        }
        if offset < activities.len()
            && let Some(intent) = activities.get((activity_start + offset) % activities.len())
        {
            let _payment = state.0.payments.lock().await;
            if Instant::now() >= deadline {
                break;
            }
            *activity_cursor = Some(intent.activity.id.clone());
            if let Some(activity) = state.0.store.activity_for_key(&intent.key)?
                && !matches!(activity.status.as_str(), "confirmed" | "failed")
                && let Err(error) =
                    progress_activity_until(state, intent, activity.clone(), deadline).await
            {
                // Progression may have persisted one or more state changes
                // before failing. Reload before applying the guarded error
                // update so it is associated with the current state and txid.
                record_current_activity_error(&state.0.store, &intent.key, &error.to_string())?;
                tracing::warn!(
                    error = %format_args!("{error:#}"),
                    activity = %activity.id,
                    "accepted payment remains pending"
                );
            }
        }

        if Instant::now() >= deadline {
            break;
        }
        if offset < address_faucets.len()
            && let Some(intent) =
                address_faucets.get((address_start + offset) % address_faucets.len())
        {
            let _payment = state.0.payments.lock().await;
            if Instant::now() >= deadline {
                break;
            }
            *address_cursor = Some(intent.payment.id.clone());
            if let Some(payment) = state.0.store.address_faucet_for_key(&intent.key)?
                && !matches!(payment.status.as_str(), "confirmed" | "failed")
                && let Err(error) = progress_address_faucet(state, payment.clone(), deadline).await
            {
                record_current_address_error(&state.0.store, &intent.key, &error.to_string())?;
                tracing::warn!(
                    error = %format_args!("{error:#}"),
                    payment = %payment.id,
                    "accepted address faucet remains pending"
                );
            }
        }
    }
    Ok(())
}

fn record_current_activity_error(store: &Store, key: &str, error: &str) -> anyhow::Result<()> {
    if let Some(current) = store.activity_for_key(key)? {
        store.record_error(&current.id, &current.status, &current.txid, error)?;
    }
    Ok(())
}

fn record_current_address_error(store: &Store, key: &str, error: &str) -> anyhow::Result<()> {
    if let Some(current) = store.address_faucet_for_key(key)? {
        store.record_address_error(&current.id, &current.status, &current.txid, error)?;
    }
    Ok(())
}

#[cfg(test)]
async fn progress_activity(
    state: &AppState,
    intent: &crate::db::ActivityIntent,
    activity: Activity,
) -> anyhow::Result<()> {
    progress_activity_until(
        state,
        intent,
        activity,
        Instant::now() + PAYMENT_DISPATCH_BUDGET,
    )
    .await
}

async fn progress_activity_until(
    state: &AppState,
    intent: &crate::db::ActivityIntent,
    mut activity: Activity,
    deadline: Instant,
) -> anyhow::Result<()> {
    loop {
        match activity.status.as_str() {
            "confirmed" | "failed" => return Ok(()),
            "prepared" | "broadcast" => {
                match submit_prepared(&state.0.store, state, &activity).await? {
                    PreparedSubmission::Broadcast(updated) => {
                        let _ = confirm_after_mining(state, updated).await?;
                        return Ok(());
                    }
                    PreparedSubmission::Expired(updated) => activity = updated,
                }
            }
            "preparing" => {
                if let Some(prepared) = repair_activity_journal(state, &activity, deadline).await? {
                    activity = state.0.store.record_prepared(
                        &activity.id,
                        &prepared.txid,
                        &prepared.raw_transaction,
                        prepared.expiry_height,
                    )?;
                    continue;
                }
                let prepared = match prepare_activity_intent(state, intent, deadline).await {
                    Ok(prepared) => prepared,
                    Err(error) if is_insufficient_funds(&error) && activity.kind == "send" => {
                        if can_fail_insufficient_send(state, &activity.id).await {
                            state.0.store.fail(&activity.id, &error.to_string())?;
                            return Ok(());
                        }
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                activity = state.0.store.record_prepared(
                    &activity.id,
                    &prepared.txid,
                    &prepared.raw_transaction,
                    prepared.expiry_height,
                )?;
            }
            status => anyhow::bail!("payment has unsupported status {status}"),
        }
    }
}

fn is_insufficient_funds(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<PaymentError>(),
        Some(PaymentError::InsufficientFunds { .. })
    )
}

async fn can_fail_insufficient_send(state: &AppState, activity_id: &str) -> bool {
    // The caller holds the payment mutex. Reading the wallet journal also
    // waits behind any preparation worker that already owns the wallet DB.
    let wallet_prepared = match state.0.wallet.prepared(activity_id).await {
        Ok(prepared) => prepared,
        Err(_) => return false,
    };
    let store_expiry = match state.0.store.prepared_transaction_expiry(activity_id) {
        Ok(expiry) => expiry,
        Err(_) => return false,
    };
    match wallet_prepared {
        None => store_expiry.is_none(),
        Some(prepared) if prepared.expiry_height != 0 => {
            let current_height = match bounded_payment_chain_info(state).await {
                Ok(info) => info.blocks,
                Err(_) => return false,
            };
            if current_height < prepared.expiry_height
                || store_expiry.is_some_and(|expiry| expiry == 0 || current_height < expiry)
            {
                return false;
            }
            matches!(
                bounded_payment_transaction_known(state, &prepared.txid).await,
                Ok(false)
            )
        }
        Some(_) => false,
    }
}

async fn repair_activity_journal(
    state: &AppState,
    activity: &Activity,
    deadline: Instant,
) -> anyhow::Result<Option<PreparedPayment>> {
    let Some(prepared) = state.0.wallet.prepared(&activity.id).await? else {
        return Ok(None);
    };
    if prepared.expiry_height == 0
        || bounded_payment_chain_info_until(state, deadline)
            .await?
            .blocks
            < prepared.expiry_height
    {
        return Ok(Some(prepared));
    }
    Ok(None)
}

async fn prepare_activity_intent(
    state: &AppState,
    intent: &crate::db::ActivityIntent,
    deadline: Instant,
) -> anyhow::Result<PreparedPayment> {
    let activity = &intent.activity;
    let sync_deadline = (Instant::now() + PAYMENT_LOOKUP_TIMEOUT).min(deadline);
    state.synchronize_wallet_until(None, sync_deadline).await?;
    let seed = state.0.store.seed()?;
    let record = record_prepared_payment(&state.0.store, &activity.id);
    match activity.kind.as_str() {
        "send" => {
            let account_id = activity
                .from_account
                .context("send omitted source account")?;
            let destination = if let Some(address) = &activity.to_address {
                address.clone()
            } else {
                let account = state
                    .0
                    .store
                    .account(activity.to_account.context("send omitted destination")?)?;
                match activity.destination_pool.as_str() {
                    "transparent" => account.transparent_address,
                    "ironwood" => account.unified_address,
                    pool => anyhow::bail!("send has unsupported destination pool {pool}"),
                }
            };
            let memo = parse_memo(intent.memo.as_deref(), &activity.destination_pool)
                .map_err(|error| anyhow::anyhow!("{}", error.message))?;
            state
                .0
                .wallet
                .prepare(
                    Some(&activity.id),
                    &seed,
                    account_id,
                    &activity.source_pool,
                    &destination,
                    activity.amount_zatoshi,
                    memo,
                    record,
                )
                .await
        }
        "faucet" => {
            let account = state.0.store.account(TREASURY_ACCOUNT_ID)?;
            let destination = state.0.store.account(
                activity
                    .to_account
                    .context("faucet omitted destination account")?,
            )?;
            let address = match activity.destination_pool.as_str() {
                "transparent" => destination.transparent_address,
                "ironwood" => destination.unified_address,
                pool => anyhow::bail!("faucet has unsupported destination pool {pool}"),
            };
            let _replenishment = state.0.treasury_replenishment.lock().await;
            prepare_with_replenishment(
                state,
                Some(&activity.id),
                &seed,
                &account,
                &address,
                activity.amount_zatoshi,
                record,
            )
            .await
        }
        kind => anyhow::bail!("unsupported accepted payment kind {kind}"),
    }
}

async fn progress_address_faucet(
    state: &AppState,
    mut payment: AddressFaucet,
    deadline: Instant,
) -> anyhow::Result<()> {
    loop {
        match payment.status.as_str() {
            "confirmed" | "failed" => return Ok(()),
            "prepared" | "broadcast" => {
                match submit_address_prepared(&state.0.store, state, &payment).await? {
                    AddressSubmission::Broadcast(updated) => {
                        let _ = confirm_address_after_mining(state, updated).await?;
                        return Ok(());
                    }
                    AddressSubmission::Expired(updated) => payment = updated,
                }
            }
            "preparing" => {
                if let Some(prepared) = repair_address_journal(state, &payment, deadline).await? {
                    payment = state.0.store.record_address_prepared(
                        &payment.id,
                        &prepared.txid,
                        &prepared.raw_transaction,
                        prepared.expiry_height,
                    )?;
                    continue;
                }
                let sync_deadline = (Instant::now() + PAYMENT_LOOKUP_TIMEOUT).min(deadline);
                state.synchronize_wallet_until(None, sync_deadline).await?;
                let seed = state.0.store.seed()?;
                let treasury = state.0.store.account(TREASURY_ACCOUNT_ID)?;
                let _replenishment = state.0.treasury_replenishment.lock().await;
                let prepared = prepare_with_replenishment(
                    state,
                    Some(&payment.id),
                    &seed,
                    &treasury,
                    &payment.address,
                    payment.amount_zatoshi,
                    record_prepared_address_payment(&state.0.store, &payment.id),
                )
                .await?;
                payment = state.0.store.record_address_prepared(
                    &payment.id,
                    &prepared.txid,
                    &prepared.raw_transaction,
                    prepared.expiry_height,
                )?;
            }
            status => anyhow::bail!("address faucet has unsupported status {status}"),
        }
    }
}

async fn repair_address_journal(
    state: &AppState,
    payment: &AddressFaucet,
    deadline: Instant,
) -> anyhow::Result<Option<PreparedPayment>> {
    let Some(prepared) = state.0.wallet.prepared(&payment.id).await? else {
        return Ok(None);
    };
    if prepared.expiry_height == 0
        || bounded_payment_chain_info_until(state, deadline)
            .await?
            .blocks
            < prepared.expiry_height
    {
        return Ok(Some(prepared));
    }
    Ok(None)
}

/// Serves the dashboard shell for client-side routes only.
///
/// The dashboard is a single-page app, so an unknown path is usually a route
/// like `/explorer/block/42` and must return the shell with `200`. It is not
/// a blanket catch-all: an unknown `/api/` path is a genuine 404, and a
/// missing asset must stay a 404 rather than returning HTML that the browser
/// would then try to parse as JavaScript or CSS.
async fn spa_fallback(uri: Uri, index: Arc<Option<String>>) -> Response {
    let path = uri.path();
    let looks_like_a_file = path
        .rsplit('/')
        .next()
        .is_some_and(|last| last.contains('.'));

    if path.starts_with("/api/") || looks_like_a_file {
        return ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("{path} does not exist"),
        }
        .into_response();
    }

    match index.as_ref() {
        Some(html) => Html(html.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, "dashboard assets are not installed").into_response(),
    }
}

/// Static assets plus the single-page fallback.
fn dashboard_router(static_dir: PathBuf) -> Router {
    // Read once: the shell is small and immutable for the life of the process.
    let index_html = Arc::new(std::fs::read_to_string(static_dir.join("index.html")).ok());
    Router::new().fallback_service(
        // `not_found_service` would wrap the fallback in `SetStatus(404)`,
        // which renders correctly but reports every deep link as missing.
        ServeDir::new(static_dir)
            .fallback((move |uri: Uri| spa_fallback(uri, Arc::clone(&index_html))).into_service()),
    )
}

pub fn router(state: AppState) -> Router {
    let static_dir = std::env::var("THS_WEB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("web/dist"));
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/status", get(status))
        .route("/api/v1/accounts", get(accounts))
        .route("/api/v1/activity", get(activity))
        .route("/api/v1/send", post(send))
        .route("/api/v1/send/quote", post(send_quote))
        .route("/api/v1/zip321/parse", post(parse_payment_uri))
        .route("/api/v1/zip321/request", post(payment_request))
        .route("/api/v1/faucet", post(faucet))
        .route(
            "/api/v1/faucet/address",
            get(faucet_address_status).post(faucet_address),
        )
        .route("/api/v1/mine", post(mine))
        .route(
            "/api/v1/mining/jobs",
            get(latest_mining_job).post(start_mining_job),
        )
        .route("/api/v1/mining/jobs/{id}", get(mining_job))
        .route("/api/v1/dev/seed", post(seed))
        .route("/api/v1/blocks", get(blocks))
        .route("/api/v1/blocks/{id}", get(block))
        .route("/api/v1/transactions/{txid}", get(transaction))
        .route("/api/v1/mempool", get(mempool))
        .route("/api/v1/addresses/{address}", get(address))
        .route("/api/v1/search", get(search))
        .route("/api/v1/events", get(events))
        .fallback_service(dashboard_router(static_dir))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn dependencies_ready(state: &AppState) -> anyhow::Result<()> {
    state.0.rpc.chain_info().await?;
    state.synchronize_latest().await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Response {
    let node = state.0.rpc.chain_info().await.ok();
    let wallet = state.wallet_sync_status().await;
    let ok = state.started() && node.is_some() && wallet.last_success_at.is_some();
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({"ok": ok, "instance": state.0.instance, "node": node, "wallet_sync": wallet})),
    )
        .into_response()
}

#[derive(Serialize)]
struct Status {
    instance: String,
    node: Option<ChainInfo>,
    account_count: usize,
    auto_mine: bool,
    network: &'static str,
    endpoints: PublicEndpoints,
    wallet_sync: WalletSyncStatus,
    #[serde(flatten)]
    settlement: SettlementStatus,
}

#[derive(Serialize)]
struct PublicEndpoints {
    dashboard: String,
    zakura_rpc: String,
    lightwalletd: String,
    p2p: String,
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Status>> {
    let dashboard_host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("127.0.0.1:8080");
    let (node, lightwalletd) =
        tokio::join!(state.0.rpc.chain_info(), state.0.wallet.latest_checkpoint());
    let node = node.ok();
    let lightwalletd = lightwalletd.ok();
    let wallet_sync = state.wallet_sync_status().await;
    let node_tip = node.as_ref().map(|chain| ChainCheckpoint {
        height: chain.blocks,
        hash: chain.bestblockhash.clone(),
    });
    let settlement = settlement_status(node_tip.as_ref(), lightwalletd.as_ref(), &wallet_sync);
    Ok(Json(Status {
        instance: state.0.instance.clone(),
        node,
        account_count: usize::from(USER_ACCOUNT_COUNT),
        auto_mine: true,
        network: "Regtest",
        endpoints: PublicEndpoints {
            dashboard: format!("http://{dashboard_host}"),
            zakura_rpc: std::env::var("THS_PUBLIC_ZAKURA_RPC")
                .unwrap_or_else(|_| "http://127.0.0.1:18232".into()),
            lightwalletd: std::env::var("THS_PUBLIC_LIGHTWALLETD")
                .unwrap_or_else(|_| "http://127.0.0.1:9067".into()),
            p2p: std::env::var("THS_PUBLIC_P2P").unwrap_or_else(|_| "127.0.0.1:18233".into()),
        },
        wallet_sync,
        settlement,
    }))
}
async fn accounts(State(state): State<AppState>) -> ApiResult<Json<Vec<Account>>> {
    let mut accounts = state.accounts().await;
    accounts.retain(|account| account.id <= USER_ACCOUNT_COUNT);
    Ok(Json(accounts))
}

#[derive(Deserialize)]
struct Page {
    limit: Option<u32>,
    idempotency_key: Option<String>,
}
async fn activity(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Json<Vec<Activity>>> {
    if let Some(key) = page.idempotency_key {
        require_key(&key)?;
        return Ok(Json(
            state.0.store.activity_for_key(&key)?.into_iter().collect(),
        ));
    }
    Ok(Json(state.0.store.activities(page.limit.unwrap_or(30))?))
}

#[derive(Deserialize)]
/// Exactly one of `to_account` or `to_address` names the destination.
struct SendRequest {
    from_account: u8,
    to_account: Option<u8>,
    to_address: Option<String>,
    source_pool: String,
    destination_pool: String,
    amount_zatoshi: u64,
    idempotency_key: String,
    #[serde(default)]
    memo: Option<String>,
}
async fn send(
    State(state): State<AppState>,
    Json(req): Json<SendRequest>,
) -> ApiResult<Json<Activity>> {
    require_started(&state)?;
    let (recipient, memo) = validate_send(&req)?;
    let address = match recipient {
        Recipient::Account(id) => {
            let destination = state.0.store.account(id)?;
            match req.destination_pool.as_str() {
                "transparent" => destination.transparent_address,
                "ironwood" => destination.unified_address,
                _ => unreachable!("validate_send checks destination_pool"),
            }
        }
        Recipient::Address(address) => address.to_owned(),
    };
    // A pasted address that belongs to a development account is recorded as
    // that account, before the claim, so a replay resolves to the same payment.
    let recipient = match recipient {
        Recipient::Address(address) => {
            development_account_for(&state.0.store, address)?.map_or(recipient, Recipient::Account)
        }
        recipient => recipient,
    };
    if recipient == Recipient::Account(req.from_account) && req.source_pool == req.destination_pool
    {
        return Err(ApiError::bad_request(
            "choose a different account or a different destination pool",
        ));
    }
    let _payment = state.0.payments.lock().await;
    let pending = state.0.store.claim_transfer(
        req.from_account,
        recipient,
        &req.source_pool,
        &req.destination_pool,
        req.amount_zatoshi,
        &req.idempotency_key,
        req.memo.as_deref(),
    )?;
    Ok(Json(
        execute_payment(&state, pending, |activity_id| {
            let state = &state;
            let memo = memo.clone();
            let req = &req;
            let address = &address;
            async move {
                state.synchronize_latest().await?;
                state
                    .0
                    .wallet
                    .prepare(
                        Some(&activity_id),
                        &state.0.store.seed()?,
                        req.from_account,
                        &req.source_pool,
                        address,
                        req.amount_zatoshi,
                        memo,
                        record_prepared_payment(&state.0.store, &activity_id),
                    )
                    .await
            }
        })
        .await?,
    ))
}

#[derive(Deserialize)]
struct PaymentUriRequest {
    uri: String,
}

/// A single-payment ZIP-321 request, resolved for the Send form.
#[derive(Debug, Serialize)]
struct PaymentUri {
    address: String,
    destination_pool: &'static str,
    /// Set when the address belongs to a development account.
    to_account: Option<u8>,
    amount_zatoshi: Option<u64>,
    memo: Option<String>,
}

async fn parse_payment_uri(
    State(state): State<AppState>,
    Json(req): Json<PaymentUriRequest>,
) -> ApiResult<Json<PaymentUri>> {
    Ok(Json(payment_uri(&state.0.store, &req.uri)?))
}

fn payment_uri(store: &Store, uri: &str) -> ApiResult<PaymentUri> {
    let request = TransactionRequest::from_uri(uri.trim()).map_err(|error| {
        tracing::debug!(%error, "rejected ZIP-321 URI");
        ApiError::bad_request("Invalid payment URI")
    })?;
    let mut payments = request.payments().values();
    let (Some(payment), None) = (payments.next(), payments.next()) else {
        return Err(ApiError::bad_request(
            "only single-payment URIs are supported",
        ));
    };
    let address = payment.recipient_address().encode();
    let destination_pool = require_send_address(&address)?;
    let memo = match payment.memo().map(Memo::try_from).transpose() {
        Ok(None | Some(Memo::Empty)) => None,
        Ok(Some(Memo::Text(text))) => Some(String::from(&*text)),
        _ => return Err(ApiError::bad_request("only text memos are supported")),
    };
    Ok(PaymentUri {
        to_account: development_account_for(store, &address)?,
        address,
        destination_pool,
        amount_zatoshi: payment.amount().map(u64::from),
        memo,
    })
}

fn development_account_for(store: &Store, address: &str) -> anyhow::Result<Option<u8>> {
    for id in 1..=USER_ACCOUNT_COUNT {
        let account = store.account(id)?;
        if account.unified_address == address || account.transparent_address == address {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

#[derive(Deserialize)]
struct PaymentRequest {
    account_id: u8,
    amount_zatoshi: Option<u64>,
    #[serde(default)]
    memo: Option<String>,
}

#[derive(Serialize)]
struct PaymentRequestResponse {
    uri: String,
}

async fn payment_request(
    State(state): State<AppState>,
    Json(req): Json<PaymentRequest>,
) -> ApiResult<Json<PaymentRequestResponse>> {
    require_user_account(req.account_id)?;
    let amount = req
        .amount_zatoshi
        .map(|amount| {
            if amount == 0 {
                return Err(ApiError::bad_request("amount must be greater than zero"));
            }
            Zatoshis::from_u64(amount).map_err(|_| {
                ApiError::bad_request(format!("amount_zatoshi must not exceed {MAX_MONEY}"))
            })
        })
        .transpose()?;
    let memo = parse_memo(req.memo.as_deref(), "ironwood")?;
    let account = state.0.store.account(req.account_id)?;
    let address = Address::decode(&regtest_network(), &account.unified_address)
        .context("stored account has an invalid unified address")?;
    let payment = Payment::new(
        address.to_zcash_address(&regtest_network()),
        amount,
        memo,
        None,
        None,
        vec![],
    )
    .context("constructing payment request")?;
    let uri = TransactionRequest::new(vec![payment])
        .context("constructing payment request")?
        .to_uri();
    Ok(Json(PaymentRequestResponse { uri }))
}

#[derive(Deserialize)]
struct SendQuoteRequest {
    from_account: u8,
    source_pool: String,
    destination_pool: String,
}
async fn send_quote(
    State(state): State<AppState>,
    Json(req): Json<SendQuoteRequest>,
) -> ApiResult<Json<SendQuote>> {
    require_user_account(req.from_account)?;
    require_pool(&req.source_pool, "source_pool")?;
    require_pool(&req.destination_pool, "destination_pool")?;
    // The fee depends only on the destination's receiver kinds, which every
    // account shares, so the source account's own address quotes the same fee.
    let destination = state.0.store.account(req.from_account)?;
    let address = if req.destination_pool == "transparent" {
        destination.transparent_address
    } else {
        destination.unified_address
    };
    Ok(Json(
        state
            .0
            .wallet
            .send_quote(req.from_account, &req.source_pool, &address)
            .await?,
    ))
}

#[derive(Deserialize)]
struct FaucetRequest {
    account_id: u8,
    pool: String,
    amount_zatoshi: u64,
    idempotency_key: String,
}
async fn faucet(
    State(state): State<AppState>,
    Json(req): Json<FaucetRequest>,
) -> ApiResult<Json<Activity>> {
    require_started(&state)?;
    require_key(&req.idempotency_key)?;
    require_user_account(req.account_id)?;
    if req.amount_zatoshi > 5 * ZATOSHIS_PER_ZEC {
        return Err(ApiError::bad_request(
            "a faucet request is limited to 5 ZEC",
        ));
    }
    if req.amount_zatoshi == 0 {
        return Err(ApiError::bad_request("amount must be greater than zero"));
    }
    require_pool(&req.pool, "pool")?;
    Ok(Json(
        fund_from_treasury(
            &state,
            req.account_id,
            &req.pool,
            req.amount_zatoshi,
            &req.idempotency_key,
        )
        .await?,
    ))
}

#[derive(Deserialize)]
struct FaucetAddressRequest {
    address: String,
    amount_zatoshi: u64,
    #[serde(default)]
    idempotency_key: String,
}

#[derive(Serialize)]
struct FaucetAddressResponse {
    address: String,
    amount_zatoshi: u64,
    txid: String,
    block_hash: Option<String>,
    status: String,
    last_error: Option<String>,
}

fn internal_faucet_destination(
    store: &Store,
    address: &str,
) -> anyhow::Result<Option<(u8, &'static str)>> {
    for account in store.accounts()? {
        if !(1..=USER_ACCOUNT_COUNT).contains(&account.id) {
            continue;
        }
        if account.transparent_address == address {
            return Ok(Some((account.id, "transparent")));
        }
        if account.unified_address == address {
            return Ok(Some((account.id, "ironwood")));
        }
    }
    Ok(None)
}

fn faucet_address_response(
    address: String,
    activity: Activity,
) -> anyhow::Result<FaucetAddressResponse> {
    let block_hash = activity.block_hash.filter(|hash| !hash.is_empty());
    if activity.status == "confirmed" {
        anyhow::ensure!(
            block_hash.is_some(),
            "faucet transaction was not included in a block"
        );
    }
    Ok(FaucetAddressResponse {
        address,
        amount_zatoshi: activity.amount_zatoshi,
        txid: activity.txid,
        block_hash,
        status: activity.status,
        last_error: activity.last_error,
    })
}

fn external_faucet_response(payment: AddressFaucet) -> FaucetAddressResponse {
    FaucetAddressResponse {
        address: payment.address,
        amount_zatoshi: payment.amount_zatoshi,
        txid: payment.txid,
        block_hash: payment.block_hash,
        status: payment.status,
        last_error: payment.last_error,
    }
}

fn stored_address_faucet(
    store: &Store,
    key: &str,
) -> anyhow::Result<Option<FaucetAddressResponse>> {
    if let Some(payment) = store.address_faucet_for_key(key)? {
        return Ok(Some(external_faucet_response(payment)));
    }
    let Some(activity) = store
        .activity_for_key(key)?
        .filter(|activity| activity.kind == "faucet")
    else {
        return Ok(None);
    };
    let account = store.account(
        activity
            .to_account
            .context("internal faucet omitted its account")?,
    )?;
    let address = match activity.destination_pool.as_str() {
        "transparent" => account.transparent_address,
        "ironwood" => account.unified_address,
        pool => anyhow::bail!("faucet has unsupported destination pool {pool}"),
    };
    faucet_address_response(address, activity).map(Some)
}

#[async_trait::async_trait]
trait AddressFaucetRuntime: Sync {
    async fn fund_internal(
        &self,
        account_id: u8,
        pool: &str,
        amount_zatoshi: u64,
        key: &str,
    ) -> anyhow::Result<Activity>;
    async fn fund_external(
        &self,
        req: FaucetAddressRequest,
    ) -> anyhow::Result<FaucetAddressResponse>;
}

#[async_trait::async_trait]
impl AddressFaucetRuntime for AppState {
    async fn fund_internal(
        &self,
        account_id: u8,
        pool: &str,
        amount_zatoshi: u64,
        key: &str,
    ) -> anyhow::Result<Activity> {
        fund_from_treasury(self, account_id, pool, amount_zatoshi, key).await
    }

    async fn fund_external(
        &self,
        req: FaucetAddressRequest,
    ) -> anyhow::Result<FaucetAddressResponse> {
        let payment = fund_address_from_treasury(
            self,
            &req.address,
            req.amount_zatoshi,
            &req.idempotency_key,
        )
        .await?;
        Ok(external_faucet_response(payment))
    }
}

async fn execute_address_faucet<R: AddressFaucetRuntime>(
    store: &Store,
    runtime: &R,
    req: FaucetAddressRequest,
) -> ApiResult<Json<FaucetAddressResponse>> {
    require_key(&req.idempotency_key)?;
    require_faucet_address(&req.address)?;
    if req.amount_zatoshi == 0 || req.amount_zatoshi > 5 * ZATOSHIS_PER_ZEC {
        return Err(ApiError::bad_request(
            "amount must be greater than zero and no more than 5 ZEC",
        ));
    }
    if store
        .address_faucet_for_key(&req.idempotency_key)?
        .is_some()
    {
        return Ok(Json(runtime.fund_external(req).await?));
    }
    if let Some((account_id, pool)) = internal_faucet_destination(store, &req.address)? {
        let activity = runtime
            .fund_internal(account_id, pool, req.amount_zatoshi, &req.idempotency_key)
            .await?;
        Ok(Json(faucet_address_response(req.address, activity)?))
    } else {
        Ok(Json(runtime.fund_external(req).await?))
    }
}

async fn faucet_address(
    State(state): State<AppState>,
    Json(req): Json<FaucetAddressRequest>,
) -> ApiResult<Json<FaucetAddressResponse>> {
    require_started(&state)?;
    execute_address_faucet(&state.0.store, &state, req).await
}

#[derive(Deserialize)]
struct FaucetAddressQuery {
    idempotency_key: String,
}
async fn faucet_address_status(
    State(state): State<AppState>,
    Query(query): Query<FaucetAddressQuery>,
) -> ApiResult<Json<FaucetAddressResponse>> {
    require_key(&query.idempotency_key)?;
    stored_address_faucet(&state.0.store, &query.idempotency_key)?
        .map(Json)
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            message: "no address faucet uses that idempotency key".into(),
        })
}

async fn fund_address_from_treasury(
    state: &AppState,
    address: &str,
    amount_zatoshi: u64,
    idempotency_key: &str,
) -> anyhow::Result<AddressFaucet> {
    let _payment = state.0.payments.lock().await;
    let mut pending =
        state
            .0
            .store
            .claim_address_faucet(address, amount_zatoshi, idempotency_key)?;
    loop {
        match pending.status.as_str() {
            "confirmed" | "failed" => return Ok(pending),
            "prepared" | "broadcast" => {
                match submit_address_prepared(&state.0.store, state, &pending).await? {
                    AddressSubmission::Broadcast(payment) => {
                        pending = payment;
                        break;
                    }
                    AddressSubmission::Expired(payment) => pending = payment,
                }
            }
            "preparing" => {
                let prepared = async {
                    state.synchronize_latest().await?;
                    let seed = state.0.store.seed()?;
                    let treasury = state.0.store.account(TREASURY_ACCOUNT_ID)?;
                    let _replenishment = state.0.treasury_replenishment.lock().await;
                    prepare_with_replenishment(
                        state,
                        Some(&pending.id),
                        &seed,
                        &treasury,
                        address,
                        amount_zatoshi,
                        record_prepared_address_payment(&state.0.store, &pending.id),
                    )
                    .await
                }
                .await;
                let prepared = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        state.0.store.record_address_error(
                            &pending.id,
                            "preparing",
                            "",
                            &error.to_string(),
                        )?;
                        return Err(error);
                    }
                };
                pending = state.0.store.record_address_prepared(
                    &pending.id,
                    &prepared.txid,
                    &prepared.raw_transaction,
                    prepared.expiry_height,
                )?;
            }
            status => anyhow::bail!("address faucet has unsupported status {status}"),
        }
    }
    confirm_address_after_mining(state, pending).await
}

async fn confirm_address_after_mining(
    state: &AppState,
    pending: AddressFaucet,
) -> anyhow::Result<AddressFaucet> {
    let check = |payment: AddressFaucet, tx: Value| -> anyhow::Result<AddressFaucet> {
        match confirmed_block_hash(&tx) {
            Some(hash) => state
                .0
                .store
                .confirm_address(&payment.id, &payment.txid, hash),
            None => Ok(payment),
        }
    };
    let pending = match state.0.rpc.transaction(&pending.txid).await {
        Ok(tx) => check(pending, tx)?,
        Err(error) => {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                txid = %pending.txid,
                "could not check address faucet confirmation"
            );
            pending
        }
    };
    if pending.status == "confirmed" {
        return Ok(pending);
    }
    if let Err(error) = mine_and_sync(state, 1).await {
        tracing::warn!(
            error = %format_args!("{error:#}"),
            payment = %pending.id,
            "address faucet recorded but auto-mine failed"
        );
        return Ok(pending);
    }
    match state.0.rpc.transaction(&pending.txid).await {
        Ok(tx) => check(pending, tx),
        Err(error) => {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                txid = %pending.txid,
                "could not check address faucet confirmation"
            );
            Ok(pending)
        }
    }
}

async fn fund_from_treasury(
    state: &AppState,
    account_id: u8,
    pool: &str,
    amount_zatoshi: u64,
    idempotency_key: &str,
) -> anyhow::Result<Activity> {
    let _payment = state.0.payments.lock().await;
    let pending = state
        .0
        .store
        .claim_faucet(account_id, pool, amount_zatoshi, idempotency_key)?;
    execute_payment(state, pending, |activity_id| async move {
        let destination = state.0.store.account(account_id)?;
        let address = match pool {
            "transparent" => destination.transparent_address,
            "ironwood" => destination.unified_address,
            _ => unreachable!("claim_faucet validates pool"),
        };
        state.synchronize_latest().await?;
        let seed = state.0.store.seed()?;
        // SDK proposals check spendability and the actual fee before construction. Total
        // balances include pending change and cannot decide whether this request is fundable.
        let treasury = state.0.store.account(TREASURY_ACCOUNT_ID)?;
        let _replenishment = state.0.treasury_replenishment.lock().await;
        prepare_with_replenishment(
            state,
            Some(&activity_id),
            &seed,
            &treasury,
            &address,
            amount_zatoshi,
            record_prepared_payment(&state.0.store, &activity_id),
        )
        .await
    })
    .await
}

#[async_trait::async_trait]
trait PaymentLifecycle: Sync {
    async fn submit_prepared(&self, activity: &Activity) -> anyhow::Result<PreparedSubmission>;
    fn record_preparation_error(&self, id: &str, error: &str) -> anyhow::Result<()>;
    fn record_prepared(&self, id: &str, prepared: &PreparedPayment) -> anyhow::Result<Activity>;
    async fn confirm_after_mining(&self, activity: Activity) -> anyhow::Result<Activity>;
    async fn fail_insufficient_send(
        &self,
        activity: &Activity,
        error: &str,
    ) -> anyhow::Result<bool> {
        let _ = (activity, error);
        Ok(false)
    }
}

#[async_trait::async_trait]
impl PaymentLifecycle for AppState {
    async fn submit_prepared(&self, activity: &Activity) -> anyhow::Result<PreparedSubmission> {
        submit_prepared(&self.0.store, self, activity).await
    }

    fn record_preparation_error(&self, id: &str, error: &str) -> anyhow::Result<()> {
        self.0.store.record_error(id, "preparing", "", error)?;
        Ok(())
    }

    fn record_prepared(&self, id: &str, prepared: &PreparedPayment) -> anyhow::Result<Activity> {
        self.0.store.record_prepared(
            id,
            &prepared.txid,
            &prepared.raw_transaction,
            prepared.expiry_height,
        )
    }

    async fn confirm_after_mining(&self, activity: Activity) -> anyhow::Result<Activity> {
        confirm_after_mining(self, activity).await
    }

    async fn fail_insufficient_send(
        &self,
        activity: &Activity,
        error: &str,
    ) -> anyhow::Result<bool> {
        if activity.status != "preparing"
            || !activity.txid.is_empty()
            || !can_fail_insufficient_send(self, &activity.id).await
        {
            return Ok(false);
        }
        self.0.store.fail(&activity.id, error)?;
        Ok(true)
    }
}

async fn execute_payment<R, Prepare, PrepareFuture>(
    runtime: &R,
    mut pending: Activity,
    mut prepare: Prepare,
) -> anyhow::Result<Activity>
where
    R: PaymentLifecycle,
    Prepare: FnMut(String) -> PrepareFuture,
    PrepareFuture: Future<Output = anyhow::Result<PreparedPayment>>,
{
    loop {
        match pending.status.as_str() {
            "confirmed" | "failed" => return Ok(pending),
            "prepared" | "broadcast" => match runtime.submit_prepared(&pending).await? {
                PreparedSubmission::Broadcast(activity) => {
                    return runtime.confirm_after_mining(activity).await;
                }
                PreparedSubmission::Expired(activity) => pending = activity,
            },
            "preparing" => {
                let prepared = match prepare(pending.id.clone()).await {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        if pending.kind == "send"
                            && is_insufficient_funds(&error)
                            && runtime
                                .fail_insufficient_send(&pending, &error.to_string())
                                .await?
                        {
                            // Preserve the current POST error response while
                            // retaining a terminal, replayable operation.
                            return Err(error);
                        }
                        runtime.record_preparation_error(&pending.id, &error.to_string())?;
                        return Err(error);
                    }
                };
                pending = runtime.record_prepared(&pending.id, &prepared)?;
            }
            status => anyhow::bail!("payment has unsupported status {status}"),
        }
    }
}

#[async_trait::async_trait]
trait PaymentSubmitter: Sync {
    async fn transaction_known(&self, txid: &str) -> anyhow::Result<bool>;
    async fn chain_height(&self) -> anyhow::Result<u64>;
    async fn broadcast(&self, raw_transaction: &[u8]) -> anyhow::Result<()>;
    async fn recover_prepared(&self, txid: &str) -> anyhow::Result<Option<PreparedPayment>>;
}

#[async_trait::async_trait]
impl PaymentSubmitter for AppState {
    async fn transaction_known(&self, txid: &str) -> anyhow::Result<bool> {
        bounded_payment_transaction_known(self, txid).await
    }

    async fn chain_height(&self) -> anyhow::Result<u64> {
        Ok(bounded_payment_chain_info(self).await?.blocks)
    }

    async fn broadcast(&self, raw_transaction: &[u8]) -> anyhow::Result<()> {
        self.0.wallet.broadcast(raw_transaction).await
    }

    async fn recover_prepared(&self, txid: &str) -> anyhow::Result<Option<PreparedPayment>> {
        self.0.wallet.recover_prepared(txid).await
    }
}

fn record_prepared_payment(
    store: &Store,
    id: &str,
) -> impl FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Clone + Send + 'static {
    let store = store.clone();
    let id = id.to_owned();
    move |prepared| {
        store.record_prepared(
            &id,
            &prepared.txid,
            &prepared.raw_transaction,
            prepared.expiry_height,
        )?;
        Ok(prepared)
    }
}

fn record_prepared_address_payment(
    store: &Store,
    id: &str,
) -> impl FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Clone + Send + 'static {
    let store = store.clone();
    let id = id.to_owned();
    move |prepared| {
        store.record_address_prepared(
            &id,
            &prepared.txid,
            &prepared.raw_transaction,
            prepared.expiry_height,
        )?;
        Ok(prepared)
    }
}

async fn submit_prepared<R: PaymentSubmitter>(
    store: &Store,
    runtime: &R,
    activity: &Activity,
) -> anyhow::Result<PreparedSubmission> {
    match submit_saved_prepared(store, runtime, &activity.id, &activity.txid, |recovered| {
        store.record_prepared(
            &activity.id,
            &activity.txid,
            &recovered.raw_transaction,
            recovered.expiry_height,
        )?;
        Ok(())
    })
    .await?
    {
        SavedSubmission::Broadcast => Ok(PreparedSubmission::Broadcast(
            store.mark_broadcast(&activity.id, &activity.txid)?,
        )),
        SavedSubmission::Expired => Ok(PreparedSubmission::Expired(
            store.reset_for_retry(&activity.id, &activity.txid)?,
        )),
    }
}

enum AddressSubmission {
    Broadcast(AddressFaucet),
    Expired(AddressFaucet),
}

async fn submit_address_prepared<R: PaymentSubmitter>(
    store: &Store,
    runtime: &R,
    payment: &AddressFaucet,
) -> anyhow::Result<AddressSubmission> {
    match submit_saved_prepared(store, runtime, &payment.id, &payment.txid, |recovered| {
        store.record_address_prepared(
            &payment.id,
            &payment.txid,
            &recovered.raw_transaction,
            recovered.expiry_height,
        )?;
        Ok(())
    })
    .await?
    {
        SavedSubmission::Broadcast => Ok(AddressSubmission::Broadcast(
            store.mark_address_broadcast(&payment.id, &payment.txid)?,
        )),
        SavedSubmission::Expired => Ok(AddressSubmission::Expired(
            store.reset_address_for_retry(&payment.id, &payment.txid)?,
        )),
    }
}

enum SavedSubmission {
    Broadcast,
    Expired,
}

async fn submit_saved_prepared<
    R: PaymentSubmitter,
    F: FnOnce(&PreparedPayment) -> anyhow::Result<()>,
>(
    store: &Store,
    runtime: &R,
    id: &str,
    txid: &str,
    record_recovered: F,
) -> anyhow::Result<SavedSubmission> {
    if runtime.transaction_known(txid).await? {
        return Ok(SavedSubmission::Broadcast);
    }
    let prepared = match store.prepared_transaction(id) {
        Ok(prepared) => prepared,
        Err(missing) => {
            let recovered = runtime.recover_prepared(txid).await?.ok_or(missing)?;
            if recovered.txid != txid {
                anyhow::bail!("wallet returned a different prepared transaction");
            }
            record_recovered(&recovered)?;
            PreparedTransaction {
                raw_transaction: recovered.raw_transaction,
                expiry_height: recovered.expiry_height,
            }
        }
    };
    if prepared.expiry_height != 0 && runtime.chain_height().await? >= prepared.expiry_height {
        if runtime.transaction_known(txid).await? {
            return Ok(SavedSubmission::Broadcast);
        }
        return Ok(SavedSubmission::Expired);
    }
    if let Err(broadcast_error) = runtime.broadcast(&prepared.raw_transaction).await {
        match runtime.transaction_known(txid).await {
            Ok(true) => {}
            Ok(false) => return Err(broadcast_error),
            Err(lookup_error) => {
                return Err(lookup_error).with_context(|| {
                    format!(
                        "broadcast failed and transaction {txid} could not be checked: {broadcast_error}"
                    )
                });
            }
        }
    }
    Ok(SavedSubmission::Broadcast)
}

enum PreparedSubmission {
    Broadcast(Activity),
    Expired(Activity),
}

#[async_trait::async_trait]
trait FaucetRuntime: Sync {
    async fn prepare_payment<F>(
        &self,
        activity_id: Option<&str>,
        seed: &str,
        destination: &str,
        amount_zatoshi: u64,
        record: F,
    ) -> anyhow::Result<PreparedPayment>
    where
        F: FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Send + 'static;
    async fn chain_height(&self) -> anyhow::Result<u64>;
    async fn block_hash(&self, height: u32) -> anyhow::Result<String>;
    async fn synchronize_latest(&self) -> anyhow::Result<()>;
    async fn treasury_cursor(&self) -> anyhow::Result<Option<crate::db::TreasuryCursor>>;
    async fn initialize_treasury_cursor(
        &self,
        receiver: &str,
        height: u32,
        hash: &str,
    ) -> anyhow::Result<crate::db::TreasuryCursor>;
    async fn discover_reward(
        &self,
        cursor: &crate::db::TreasuryCursor,
        treasury: &Account,
    ) -> anyhow::Result<crate::db::TreasuryCursor>;
    async fn shield_coinbase(
        &self,
        seed: &str,
        treasury: &Account,
        minimum_net: u64,
    ) -> anyhow::Result<()>;
    async fn mine_and_sync(&self, blocks: u32) -> anyhow::Result<()>;
    async fn mine_to_height(&self, height: u64) -> anyhow::Result<()> {
        let current = self.chain_height().await?;
        if height > current {
            self.mine_and_sync(u32::try_from(height - current)?).await?;
        }
        anyhow::ensure!(
            self.chain_height().await? >= height,
            "maturity mining did not reach height {height}"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl FaucetRuntime for AppState {
    async fn prepare_payment<F>(
        &self,
        activity_id: Option<&str>,
        seed: &str,
        destination: &str,
        amount_zatoshi: u64,
        record: F,
    ) -> anyhow::Result<PreparedPayment>
    where
        F: FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Send + 'static,
    {
        self.0
            .wallet
            .prepare(
                activity_id,
                seed,
                TREASURY_ACCOUNT_ID,
                "ironwood",
                destination,
                amount_zatoshi,
                None,
                record,
            )
            .await
    }

    async fn chain_height(&self) -> anyhow::Result<u64> {
        Ok(self.0.rpc.chain_info().await?.blocks)
    }

    async fn block_hash(&self, height: u32) -> anyhow::Result<String> {
        self.0.rpc.block_hash(height).await
    }
    async fn synchronize_latest(&self) -> anyhow::Result<()> {
        AppState::synchronize_latest(self).await
    }
    async fn treasury_cursor(&self) -> anyhow::Result<Option<crate::db::TreasuryCursor>> {
        self.0.wallet.treasury_cursor().await
    }
    async fn initialize_treasury_cursor(
        &self,
        receiver: &str,
        height: u32,
        hash: &str,
    ) -> anyhow::Result<crate::db::TreasuryCursor> {
        self.0
            .wallet
            .initialize_treasury_cursor(receiver, height, hash)
            .await
    }
    async fn discover_reward(
        &self,
        cursor: &crate::db::TreasuryCursor,
        treasury: &Account,
    ) -> anyhow::Result<crate::db::TreasuryCursor> {
        let _sync = self.0.wallet_sync.lock().await;
        discover_reward(self, cursor, treasury).await
    }
    async fn shield_coinbase(
        &self,
        seed: &str,
        treasury: &Account,
        minimum_net: u64,
    ) -> anyhow::Result<()> {
        self.0
            .wallet
            .shield_coinbase(
                seed,
                TREASURY_ACCOUNT_ID,
                &treasury.transparent_address,
                &treasury.unified_address,
                minimum_net,
            )
            .await?;
        Ok(())
    }

    async fn mine_and_sync(&self, blocks: u32) -> anyhow::Result<()> {
        mine_and_sync(self, blocks).await?;
        Ok(())
    }
}

async fn discover_reward(
    state: &AppState,
    cursor: &crate::db::TreasuryCursor,
    treasury: &Account,
) -> anyhow::Result<crate::db::TreasuryCursor> {
    let next_height = cursor
        .height
        .checked_add(1)
        .context("treasury discovery height overflow")?;
    let block_hash = state.0.rpc.block_hash(next_height).await?;
    let block = state.0.rpc.block(&block_hash).await?;
    anyhow::ensure!(
        block.get("height").and_then(Value::as_u64) == Some(u64::from(next_height)),
        "Zakura returned the wrong treasury discovery height"
    );
    let candidate = coinbase_candidate(&block, &treasury.transparent_address)?;
    let mut required_spenders = Vec::new();
    if let Some(candidate) = &candidate {
        for index in &candidate.output_indexes {
            if state
                .0
                .rpc
                .unspent_output(&candidate.txid, *index)
                .await?
                .is_none()
            {
                let spenders = state
                    .0
                    .wallet
                    .known_spending_transactions(&candidate.txid, *index)
                    .await?;
                let mut accepted = Vec::new();
                for txid in spenders {
                    let Some(transaction) = state.0.rpc.lookup_transaction(&txid).await? else {
                        continue;
                    };
                    if let Some(hash) = confirmed_block_hash(&transaction) {
                        let height = state
                            .0
                            .rpc
                            .block(hash)
                            .await?
                            .get("height")
                            .and_then(Value::as_u64)
                            .context("spender block omitted its height")?;
                        anyhow::ensure!(
                            state.0.rpc.block_hash(u32::try_from(height)?).await? == hash,
                            "spender block is not canonical"
                        );
                    } else if !state.0.rpc.mempool().await?.contains(&txid) {
                        continue;
                    }
                    accepted.push((*index, txid));
                }
                anyhow::ensure!(
                    !accepted.is_empty(),
                    "treasury output has no known spender; discovery remains pending"
                );
                required_spenders.extend(accepted);
            }
        }
    }
    let checkpoint = ChainCheckpoint {
        height: u64::from(next_height),
        hash: block_hash,
    };
    anyhow::ensure!(
        state.0.rpc.block_hash(next_height).await? == checkpoint.hash,
        "canonical block changed during treasury discovery"
    );
    let next = state
        .0
        .wallet
        .commit_treasury_discovery(
            cursor,
            &checkpoint,
            candidate.as_ref().map(|value| value.raw.as_str()),
            &required_spenders,
        )
        .await?;
    anyhow::ensure!(
        state.0.rpc.block_hash(next_height).await? == checkpoint.hash,
        "canonical block changed after treasury discovery"
    );
    Ok(next)
}

async fn prepare_with_replenishment<R, F>(
    runtime: &R,
    activity_id: Option<&str>,
    seed: &str,
    treasury: &Account,
    destination: &str,
    amount_zatoshi: u64,
    record: F,
) -> anyhow::Result<PreparedPayment>
where
    R: FaucetRuntime,
    F: FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Clone + Send + 'static,
{
    let mut reconciled_after_scan_required = false;
    loop {
        match runtime
            .prepare_payment(
                activity_id,
                seed,
                destination,
                amount_zatoshi,
                record.clone(),
            )
            .await
        {
            Ok(prepared) => return Ok(prepared),
            Err(error) => {
                if matches!(error.downcast_ref(), Some(PaymentError::ScanRequired)) {
                    if !reconciled_after_scan_required {
                        runtime.synchronize_latest().await?;
                        reconciled_after_scan_required = true;
                        continue;
                    }
                    if runtime.chain_height().await? < u64::from(WALLET_BIRTHDAY_HEIGHT) {
                        runtime
                            .mine_to_height(u64::from(WALLET_BIRTHDAY_HEIGHT))
                            .await?;
                        runtime.synchronize_latest().await?;
                        continue;
                    }
                    return Err(error);
                }
                let Some(PaymentError::InsufficientFunds {
                    available,
                    required,
                }) = error.downcast_ref()
                else {
                    return Err(error);
                };
                let minimum_net = required
                    .checked_sub(*available)
                    .filter(|value| *value > 0)
                    .context("SDK insufficient-funds result has no positive deficit")?;
                replenish_treasury(runtime, seed, treasury, minimum_net).await?;
                reconciled_after_scan_required = false;
            }
        }
    }
}

async fn replenish_treasury<R: FaucetRuntime>(
    runtime: &R,
    seed: &str,
    treasury: &Account,
    minimum_net: u64,
) -> anyhow::Result<()> {
    let boundary = WALLET_BIRTHDAY_HEIGHT - 1;
    runtime.mine_to_height(u64::from(boundary)).await?;
    let mut cursor = match runtime.treasury_cursor().await? {
        Some(cursor) => {
            anyhow::ensure!(
                cursor.receiver == treasury.transparent_address,
                "treasury cursor belongs to a different receiver"
            );
            cursor
        }
        None => {
            let hash = runtime.block_hash(boundary).await?;
            runtime
                .initialize_treasury_cursor(&treasury.transparent_address, boundary, &hash)
                .await?
        }
    };
    if runtime.chain_height().await? < u64::from(cursor.height)
        || runtime.block_hash(cursor.height).await? != cursor.block_hash
    {
        runtime.synchronize_latest().await?;
        cursor = runtime
            .treasury_cursor()
            .await?
            .context("treasury cursor disappeared during rewind")?;
        anyhow::ensure!(
            runtime.chain_height().await? >= u64::from(cursor.height)
                && runtime.block_hash(cursor.height).await? == cursor.block_hash,
            "treasury cursor is not canonical after wallet rewind"
        );
    }

    loop {
        match runtime.shield_coinbase(seed, treasury, minimum_net).await {
            Ok(()) => return runtime.mine_and_sync(1).await,
            Err(error) if matches!(error.downcast_ref(), Some(PaymentError::TreasuryExhausted)) => {
            }
            Err(error) => return Err(error),
        }
        let next_height = cursor
            .height
            .checked_add(1)
            .context("treasury discovery height overflow")?;
        // The SDK constructs a spend at tip + 1; 100 confirmations make this reward eligible there.
        let required_height = u64::from(next_height) + u64::from(COINBASE_MATURITY_BLOCKS) - 1;
        runtime.mine_to_height(required_height).await?;
        cursor = runtime.discover_reward(&cursor, treasury).await?;
    }
}

struct CoinbaseCandidate {
    txid: String,
    raw: String,
    output_indexes: Vec<u32>,
}

fn coinbase_candidate(block: &Value, receiver: &str) -> anyhow::Result<Option<CoinbaseCandidate>> {
    let height = block
        .get("height")
        .and_then(Value::as_u64)
        .and_then(|height| u32::try_from(height).ok())
        .context("Zakura block omitted its height")?;
    let transaction = block
        .get("tx")
        .and_then(Value::as_array)
        .and_then(|transactions| transactions.first())
        .context("Zakura block omitted its coinbase transaction")?;
    let raw = transaction
        .get("hex")
        .and_then(Value::as_str)
        .context("Zakura coinbase omitted its raw transaction")?;
    let bytes = hex::decode(raw).context("invalid treasury transaction hex")?;
    let decoded = zcash_primitives::transaction::Transaction::read(
        &bytes[..],
        zcash_protocol::consensus::BranchId::for_height(&regtest_network(), height.into()),
    )?;
    let txid = decoded.txid().to_string();
    anyhow::ensure!(
        transaction.get("txid").and_then(Value::as_str) == Some(txid.as_str()),
        "Zakura coinbase transaction ID does not match its raw transaction"
    );
    let bundle = decoded
        .transparent_bundle()
        .filter(|bundle| bundle.is_coinbase())
        .context("Zakura block's first transaction is not coinbase")?;
    let output_indexes = bundle
        .vout
        .iter()
        .enumerate()
        .filter_map(|(index, output)| {
            output
                .recipient_address()
                .filter(|address| address.encode(&regtest_network()) == receiver)
                .map(|_| u32::try_from(index).context("treasury output index exceeds u32"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if output_indexes.is_empty() {
        return Ok(None);
    }
    Ok(Some(CoinbaseCandidate {
        txid,
        raw: raw.to_owned(),
        output_indexes,
    }))
}

async fn mine_and_sync(state: &AppState, blocks: u32) -> anyhow::Result<Vec<String>> {
    let hashes = state.0.rpc.generate(blocks).await?;
    let tip_hash = hashes
        .last()
        .context("Zakura did not return the mined block hash")?;
    let tip_height = state
        .0
        .rpc
        .block(tip_hash)
        .await?
        .pointer("/height")
        .and_then(Value::as_u64)
        .context("Zakura mined block omitted its height")?;
    state.synchronize_wallet(Some(tip_height)).await?;
    // Every caller here produces blocks, so the chain moved for everyone, not
    // just the tab that asked. Without this, other dashboards keep the old
    // height and tip until something else happens to mine.
    notify(state, "chain");
    Ok(hashes)
}

pub async fn provision_initial_balance(state: &AppState) -> anyhow::Result<()> {
    const INITIAL_FUNDING_KEY: &str = "startup-account-1-ironwood-v1";
    let existing = state.0.store.activity_for_key(INITIAL_FUNDING_KEY)?;
    if existing
        .as_ref()
        .is_some_and(|activity| activity.status == "confirmed")
    {
        return Ok(());
    }
    if existing.is_none() {
        // A fresh wallet needs scanned blocks before a proposal can determine its target height.
        let seed = state.0.store.seed()?;
        let treasury = state.0.store.account(TREASURY_ACCOUNT_ID)?;
        replenish_treasury(state, &seed, &treasury, 1).await?;
    }
    let funding = fund_from_treasury(
        state,
        1,
        "ironwood",
        5 * ZATOSHIS_PER_ZEC,
        INITIAL_FUNDING_KEY,
    )
    .await?;
    if funding.status != "confirmed" {
        return Err(crate::ReadinessError::FundingPending.into());
    }
    let account = state
        .accounts()
        .await
        .into_iter()
        .find(|account| account.id == 1)
        .context("Account 1 disappeared during startup provisioning")?;
    anyhow::ensure!(
        account.ironwood_zatoshi == 5 * ZATOSHIS_PER_ZEC,
        "Account 1 startup Ironwood balance is {}, expected {} zatoshi",
        account.ironwood_zatoshi,
        5 * ZATOSHIS_PER_ZEC
    );
    Ok(())
}

struct ManualMiningAdapter(AppState);

#[async_trait::async_trait]
impl MiningRuntime for ManualMiningAdapter {
    async fn generate_one(&self) -> anyhow::Result<String> {
        let mut hashes = self.0.0.rpc.generate(1).await?;
        anyhow::ensure!(
            hashes.len() == 1 && !hashes[0].is_empty(),
            "Zakura must return exactly one nonempty mined block hash"
        );
        Ok(hashes.remove(0))
    }

    async fn synchronize(&self, tip_hash: &str) -> anyhow::Result<()> {
        let height = self
            .0
            .0
            .rpc
            .block(tip_hash)
            .await?
            .pointer("/height")
            .and_then(Value::as_u64)
            .context("Zakura mined block omitted its height")?;
        self.0.synchronize_wallet(Some(height)).await
    }

    fn notify(&self, topic: &'static str) {
        notify(&self.0, topic);
    }
}

impl AppState {
    async fn start_mining(&self, blocks: u32, key: String) -> ApiResult<Admission> {
        let runtime: Arc<dyn MiningRuntime> = Arc::new(ManualMiningAdapter(self.clone()));
        #[cfg(test)]
        let runtime = self.0.mining_runtime.clone().unwrap_or(runtime);
        Ok(self.0.mining.start(runtime, blocks, key).await?)
    }
}

#[derive(Deserialize)]
struct StartMiningRequest {
    blocks: u32,
    idempotency_key: String,
}

async fn start_mining_job(
    State(state): State<AppState>,
    Json(req): Json<StartMiningRequest>,
) -> ApiResult<(StatusCode, Json<MiningJob>)> {
    require_started(&state)?;
    require_key(&req.idempotency_key)?;
    let admission = state.start_mining(req.blocks, req.idempotency_key).await?;
    Ok((StatusCode::ACCEPTED, Json(admission.job)))
}

async fn latest_mining_job(State(state): State<AppState>) -> Json<Value> {
    Json(json!({"job":state.0.mining.latest().await}))
}

async fn mining_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<MiningJob>> {
    state
        .0
        .mining
        .get(&id)
        .await
        .map(Json)
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            message: "Mining job does not exist".into(),
        })
}

#[derive(Deserialize)]
struct MineRequest {
    blocks: u32,
}
async fn mine(
    State(state): State<AppState>,
    Json(req): Json<MineRequest>,
) -> ApiResult<Json<Value>> {
    require_started(&state)?;
    if !(1..=10_000).contains(&req.blocks) {
        return Err(ApiError::bad_request("blocks must be between 1 and 10,000"));
    }
    let mut admission = state
        .start_mining(req.blocks, uuid::Uuid::new_v4().to_string())
        .await?;
    loop {
        let update = admission.updates.borrow_and_update().clone();
        match update.job.state {
            MiningState::Completed => {
                let hashes = update
                    .hashes
                    .context("Completed mining job omitted its hashes")?;
                return Ok(Json(json!({"blocks":hashes.len(),"hashes":*hashes})));
            }
            MiningState::Failed => {
                return Err(anyhow::anyhow!(
                    update
                        .job
                        .error
                        .context("Failed mining job omitted its error")?
                )
                .into());
            }
            MiningState::Mining | MiningState::Syncing => {}
        }
        admission
            .updates
            .changed()
            .await
            .context("Mining job update channel closed before completion")?;
    }
}

#[derive(Deserialize)]
struct SeedRequest {
    confirmation: String,
}
async fn seed(
    State(state): State<AppState>,
    Json(req): Json<SeedRequest>,
) -> ApiResult<Json<Value>> {
    if req.confirmation != "I understand this seed is for regtest only" {
        return Err(ApiError::bad_request("exact confirmation phrase required"));
    }
    Ok(Json(
        json!({"seed_hex":state.0.store.seed()?,"warning":"Never send real funds to this development seed."}),
    ))
}
async fn block(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let mut block = state
        .0
        .rpc
        .block(&id)
        .await
        .map_err(|e| not_found(e, NO_BLOCK))?;
    // `getblock` has no Ironwood root, but `z_gettreestate` does. The root is
    // an extra detail, so a failed lookup still returns the block.
    if let Some(hash) = block.get("hash").and_then(Value::as_str).map(str::to_owned) {
        match state.0.rpc.treestate(&hash).await {
            Ok(treestate) => add_ironwood_root(&mut block, &treestate),
            Err(error) => tracing::warn!(
                error = %format_args!("{error:#}"),
                %hash,
                "reading the Ironwood treestate failed"
            ),
        }
    }
    Ok(Json(block))
}
/// Copies the Ironwood note commitment root from a `z_gettreestate` response
/// into the block as `finalironwoodroot`, next to Zakura's `finalorchardroot`.
/// The genesis block (before NU6.3 activates at height 1) has no Ironwood root and
/// stays unchanged.
fn add_ironwood_root(block: &mut Value, treestate: &Value) {
    let root = treestate
        .pointer("/ironwood/commitments/finalRoot")
        .and_then(Value::as_str);
    if let (Some(root), Some(block)) = (root, block.as_object_mut()) {
        block.insert("finalironwoodroot".to_owned(), Value::from(root));
    }
}
#[derive(Deserialize)]
struct BlocksQuery {
    limit: Option<u32>,
    before: Option<u64>,
}
async fn blocks(
    State(state): State<AppState>,
    Query(query): Query<BlocksQuery>,
) -> ApiResult<Json<Value>> {
    let info = state.0.rpc.chain_info().await?;
    let end = query.before.unwrap_or(info.blocks).min(info.blocks);
    let limit = query.limit.unwrap_or(20).clamp(1, 50) as u64;
    let start = end.saturating_sub(limit.saturating_sub(1));
    let mut page = Vec::new();
    for height in (start..=end).rev() {
        page.push(state.0.rpc.block(&height.to_string()).await?);
    }
    Ok(Json(
        json!({"blocks":page,"next_before":start.checked_sub(1)}),
    ))
}
fn transparent_prevout_ids(tx: &Value) -> Vec<String> {
    let Some(vin) = tx.get("vin").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for input in vin {
        if input.get("coinbase").is_some() {
            continue;
        }
        let Some(txid) = input.get("txid").and_then(Value::as_str) else {
            continue;
        };
        if !ids.iter().any(|id| id == txid) {
            ids.push(txid.to_owned());
        }
    }
    ids
}

/// Node `getrawtransaction` does not include the spent output. Copy address
/// and value from the previous transaction so the explorer can list inputs.
fn attach_transparent_prevouts(
    tx: &mut Value,
    prev_txs: &std::collections::HashMap<String, Value>,
) {
    let Some(vin) = tx.get_mut("vin").and_then(Value::as_array_mut) else {
        return;
    };
    for input in vin {
        if input.get("coinbase").is_some() {
            continue;
        }
        let Some(prev_txid) = input.get("txid").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        let Some(n) = input.get("vout").and_then(Value::as_u64) else {
            continue;
        };
        let Some(prev) = prev_txs.get(&prev_txid) else {
            continue;
        };
        let Some(vout) = prev.get("vout").and_then(Value::as_array) else {
            continue;
        };
        let Some(out) = vout
            .iter()
            .find(|out| out.get("n").and_then(Value::as_u64) == Some(n))
            .or_else(|| vout.get(n as usize))
        else {
            continue;
        };
        if let Some(value_zat) = out.get("valueZat").cloned() {
            input["valueZat"] = value_zat;
        }
        if let Some(script) = out.get("scriptPubKey").cloned() {
            input["scriptPubKey"] = script;
        }
    }
}

async fn transaction(
    State(state): State<AppState>,
    Path(txid): Path<String>,
) -> ApiResult<Json<Value>> {
    let mut tx = state
        .0
        .rpc
        .transaction(&txid)
        .await
        .map_err(|e| not_found(e, NO_TRANSACTION))?;
    let mut prev_txs = std::collections::HashMap::new();
    for prev_id in transparent_prevout_ids(&tx) {
        if let Ok(prev) = state.0.rpc.transaction(&prev_id).await {
            prev_txs.insert(prev_id, prev);
        }
    }
    attach_transparent_prevouts(&mut tx, &prev_txs);
    Ok(Json(tx))
}
async fn mempool(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({"transactions":state.0.rpc.mempool().await?})))
}
async fn address(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> ApiResult<Json<Value>> {
    require_transparent_address(&address)?;
    let balance: Value = state
        .0
        .rpc
        .call("getaddressbalance", json!([{"addresses":[address]}]))
        .await?;
    Ok(Json(json!({"address":address,"balance":balance})))
}
#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}
async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> ApiResult<Json<Value>> {
    if query.q.starts_with('t') {
        return address(State(state), Path(query.q)).await;
    }
    // Only a 64-character hash can also be a txid; anything else is answered by the block lookup.
    // Other node failures must surface rather than fall through to the transaction lookup.
    match state.0.rpc.block(&query.q).await {
        Ok(block) => return Ok(Json(json!({"type":"block","value":block}))),
        Err(error) => {
            let error = not_found(error, NO_BLOCK);
            if query.q.len() != 64 || error.status != StatusCode::NOT_FOUND {
                return Err(error);
            }
        }
    }
    let tx = state
        .0
        .rpc
        .transaction(&query.q)
        .await
        .map_err(|e| not_found(e, NO_BLOCK_OR_TRANSACTION))?;
    Ok(Json(json!({"type":"transaction","value":tx})))
}

const NO_BLOCK: &str = "No block at that height or hash on this chain.";
const NO_TRANSACTION: &str =
    "No transaction with that ID on this chain. It may not have been mined yet.";
const NO_BLOCK_OR_TRANSACTION: &str = "No block or transaction on this chain has that hash.";

/// Zakura answers an unknown or unparseable block or transaction with -5 or -8.
fn not_found(error: anyhow::Error, message: &str) -> ApiError {
    match error.downcast_ref::<RpcError>().map(|error| error.code) {
        Some(-5 | -8) => ApiError {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        },
        _ => error.into(),
    }
}

async fn events(
    State(state): State<AppState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let mut receiver = state.0.events.subscribe();
    let stream = async_stream::stream! { loop { match receiver.recv().await { Ok(data) => yield Ok(Event::default().event("update").data(data)), Err(broadcast::error::RecvError::Lagged(_)) => yield Ok(Event::default().event("update").data("resync")), Err(_) => break } } };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn confirmed_block_hash(tx: &Value) -> Option<&str> {
    let confirmations = tx.get("confirmations").and_then(Value::as_u64).unwrap_or(0);
    if confirmations == 0 {
        return None;
    }
    tx.get("blockhash")
        .and_then(Value::as_str)
        .filter(|hash| !hash.is_empty())
}

fn apply_confirmation(store: &Store, pending: &Activity, tx: &Value) -> anyhow::Result<Activity> {
    match confirmed_block_hash(tx) {
        Some(hash) => store.confirm(&pending.id, &pending.txid, hash),
        None => Ok(pending.clone()),
    }
}

async fn confirm_from_chain(state: &AppState, pending: Activity) -> anyhow::Result<Activity> {
    if pending.status == "confirmed" {
        return Ok(pending);
    }
    match state.0.rpc.transaction(&pending.txid).await {
        Ok(tx) => {
            let updated = apply_confirmation(&state.0.store, &pending, &tx)?;
            if updated.status == "confirmed" {
                notify(state, "wallet");
            }
            Ok(updated)
        }
        Err(error) => {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                txid = %pending.txid,
                "could not fetch transaction for confirmation"
            );
            Ok(pending)
        }
    }
}

async fn reconcile_unconfirmed_with<F, Fut>(
    store: &Store,
    cursor: &mut Option<String>,
    deadline: Instant,
    mut lookup: F,
) -> anyhow::Result<bool>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<Value>>>,
{
    let mut activities = store.unconfirmed_activities()?;
    if activities.is_empty() {
        *cursor = None;
        return Ok(false);
    }
    // Stable ID ordering retains the resume point even if the last attempted
    // record was confirmed and has disappeared from the unresolved set.
    activities.sort_by(|left, right| left.id.cmp(&right.id));
    let start = cursor.as_ref().map_or(0, |id| {
        activities.partition_point(|activity| activity.id.as_str() <= id.as_str())
            % activities.len()
    });
    let mut changed = false;
    for offset in 0..activities.len() {
        if Instant::now() >= deadline {
            break;
        }
        let activity = &activities[(start + offset) % activities.len()];
        // Advance even after an uncertain lookup, so an outage for one record
        // cannot monopolize every subsequent bounded pass.
        *cursor = Some(activity.id.clone());
        let attempt_deadline = deadline.min(Instant::now() + PAYMENT_LOOKUP_TIMEOUT);
        let result: anyhow::Result<bool> = async {
            let transaction = crate::reconcile::within_deadline(
                attempt_deadline,
                "checking payment confirmation",
                lookup(activity.txid.clone()),
            )
            .await?;
            if let Some(transaction) = transaction {
                let updated = apply_confirmation(store, activity, &transaction)?;
                Ok(updated.status == "confirmed")
            } else {
                Ok(false)
            }
        }
        .await;
        match result {
            Ok(confirmed) => changed |= confirmed,
            Err(error) => tracing::warn!(
                error = %format_args!("{error:#}"),
                activity = %activity.id,
                txid = %activity.txid,
                "payment confirmation remains pending"
            ),
        }
    }
    Ok(changed)
}

async fn confirm_after_mining(state: &AppState, pending: Activity) -> anyhow::Result<Activity> {
    let pending = confirm_from_chain(state, pending).await?;
    if pending.status == "confirmed" {
        return Ok(pending);
    }
    match mine_and_sync(state, 1).await {
        Ok(_) => confirm_from_chain(state, pending).await,
        // Startup must classify the original failure; replacing it with a
        // pending result would also retry store errors and broken invariants.
        Err(error) if !state.started() => Err(error),
        Err(error) => {
            tracing::warn!(
                error = %format_args!("{error:#}"),
                activity = %pending.id,
                "transaction recorded but auto-mine failed"
            );
            Ok(pending)
        }
    }
}
fn notify(state: &AppState, topic: &str) {
    let _ = state.0.events.send(topic.to_owned());
}
fn require_started(state: &AppState) -> ApiResult<()> {
    if state.started() {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "the environment is still starting".into(),
        })
    }
}
fn require_key(key: &str) -> ApiResult<()> {
    validate_idempotency_key(key).map_err(ApiError::bad_request)
}

fn require_pool(pool: &str, field: &str) -> ApiResult<()> {
    match pool {
        "transparent" | "ironwood" => Ok(()),
        // NU6.3 is active from block 1, so the Orchard pool can neither receive
        // nor hold funds on this chain.
        "orchard" => Err(ApiError::bad_request(format!(
            "{field} orchard is unavailable: NU6.3 is active and Orchard no longer accepts deposits; use ironwood"
        ))),
        _ => Err(ApiError::bad_request(format!(
            "{field} must be transparent or ironwood"
        ))),
    }
}

/// Checks the whole send request without touching the store, wallet or node,
/// and returns the encoded memo it carries.
fn validate_send(req: &SendRequest) -> ApiResult<(Recipient<'_>, Option<MemoBytes>)> {
    require_key(&req.idempotency_key)?;
    require_user_account(req.from_account)?;
    require_pool(&req.source_pool, "source_pool")?;
    require_pool(&req.destination_pool, "destination_pool")?;
    let recipient = match (req.to_account, req.to_address.as_deref()) {
        (Some(to_account), None) => {
            require_user_account(to_account)?;
            if req.from_account == to_account && req.source_pool == req.destination_pool {
                return Err(ApiError::bad_request(
                    "choose a different account or a different destination pool",
                ));
            }
            Recipient::Account(to_account)
        }
        (None, Some(address)) => {
            if require_send_address(address)? != req.destination_pool {
                return Err(ApiError::bad_request(
                    "destination_pool does not match the destination address",
                ));
            }
            Recipient::Address(address)
        }
        _ => {
            return Err(ApiError::bad_request(
                "provide exactly one of to_account or to_address",
            ));
        }
    };
    if req.amount_zatoshi == 0 || req.amount_zatoshi > MAX_MONEY {
        return Err(ApiError::bad_request(format!(
            "amount_zatoshi must be between 1 and {MAX_MONEY}"
        )));
    }
    let memo = parse_memo(req.memo.as_deref(), &req.destination_pool)?;
    Ok((recipient, memo))
}

/// An absent (or null) memo is `None`. Any present memo, including `""`, is
/// encoded as an explicit ZIP-302 text memo and is only valid for ironwood.
fn parse_memo(memo: Option<&str>, destination_pool: &str) -> ApiResult<Option<MemoBytes>> {
    let Some(text) = memo else {
        return Ok(None);
    };
    if destination_pool != "ironwood" {
        return Err(anyhow::Error::new(PaymentError::TransparentMemo).into());
    }
    // Text memos are zero-padded to 512 bytes, so a trailing NUL could not be
    // told apart from padding and would be silently lost on decode.
    if text.ends_with('\0') {
        return Err(ApiError::bad_request(
            "memo must not end with a NUL (U+0000) character",
        ));
    }
    MemoBytes::from_bytes(text.as_bytes())
        .map(Some)
        .map_err(|error| ApiError::bad_request(format!("invalid memo: {error}")))
}

fn require_user_account(id: u8) -> ApiResult<()> {
    if (1..=USER_ACCOUNT_COUNT).contains(&id) {
        Ok(())
    } else {
        Err(ApiError::bad_request("account must be between 1 and 5"))
    }
}

fn require_faucet_address(value: &str) -> ApiResult<()> {
    match Address::decode(&regtest_network(), value) {
        Some(Address::Unified(_) | Address::Transparent(_)) => Ok(()),
        Some(_) => Err(ApiError::bad_request(
            "destination must be a unified or transparent Regtest address",
        )),
        None => Err(ApiError::bad_request(
            "destination is not a valid Regtest address",
        )),
    }
}

/// Returns the pool a payment to `value` lands in. Ironwood shares the Orchard
/// receiver, so a unified address needs one to receive shielded funds.
fn require_send_address(value: &str) -> ApiResult<&'static str> {
    match Address::decode(&regtest_network(), value) {
        Some(Address::Unified(address)) if address.has_orchard() => Ok("ironwood"),
        Some(Address::Transparent(_)) => Ok("transparent"),
        Some(Address::Unified(_)) => Err(ApiError::bad_request(
            "unified destination must include an Ironwood receiver",
        )),
        Some(_) => Err(ApiError::bad_request(
            "destination must be a unified or transparent Regtest address",
        )),
        None => Err(ApiError::bad_request(
            "destination is not a valid Regtest address",
        )),
    }
}

fn require_transparent_address(value: &str) -> ApiResult<()> {
    match Address::decode(&regtest_network(), value) {
        Some(Address::Transparent(_)) => Ok(()),
        _ => Err(ApiError::bad_request(
            "only transparent addresses have public explorer activity",
        )),
    }
}

type ApiResult<T> = Result<T, ApiError>;
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}
impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}
impl From<MiningAdmissionError> for ApiError {
    fn from(error: MiningAdmissionError) -> Self {
        let status = match error {
            MiningAdmissionError::InvalidBlocks | MiningAdmissionError::InvalidKey => {
                StatusCode::BAD_REQUEST
            }
            MiningAdmissionError::KeyConflict | MiningAdmissionError::Busy => StatusCode::CONFLICT,
            MiningAdmissionError::Capacity => StatusCode::SERVICE_UNAVAILABLE,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let status = if error.downcast_ref::<IdempotencyConflict>().is_some() {
            StatusCode::CONFLICT
        } else {
            match error.downcast_ref::<PaymentError>() {
                Some(PaymentError::TreasuryExhausted) => StatusCode::SERVICE_UNAVAILABLE,
                Some(PaymentError::InsufficientFunds { .. }) => StatusCode::UNPROCESSABLE_ENTITY,
                Some(PaymentError::TransparentMemo) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            }
        };
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %format_args!("{error:#}"), "request failed");
        }
        Self {
            status,
            message: error.to_string(),
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error":{"message":self.message,"status":self.status.as_u16()}})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
    };

    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[derive(Clone, Default)]
    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs<T>(operation: impl FnOnce() -> T) -> (T, String) {
        let output = LogWriter::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish();
        // Keep capture scoped to the synchronous operation so parallel tests and
        // tasks running on other threads cannot write into this test's log.
        let result = tracing::subscriber::with_default(subscriber, operation);
        let logs = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        (result, logs)
    }

    /// A dashboard directory containing a recognisable shell and one asset.
    fn dashboard() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            dir.path().join("index.html"),
            "<!doctype html><div id=\"root\">",
        )
        .expect("write shell");
        std::fs::create_dir(dir.path().join("assets")).expect("assets dir");
        std::fs::write(dir.path().join("assets/app.js"), "console.log(1)").expect("write asset");
        dir
    }

    async fn get(dir: &tempfile::TempDir, path: &str) -> (StatusCode, String) {
        let response = dashboard_router(dir.path().to_path_buf())
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// The dashboard is a single-page app: a path it owns is a route, not a
    /// missing file. This previously returned 404 for every path but `/`,
    /// because `not_found_service` wraps the fallback in `SetStatus(404)`.
    #[tokio::test]
    async fn client_side_routes_return_the_shell() {
        let dir = dashboard();
        for path in [
            "/",
            "/wallet",
            "/explorer",
            "/explorer/block/1209",
            "/network",
        ] {
            let (status, body) = get(&dir, path).await;
            assert_eq!(status, StatusCode::OK, "{path} should serve the shell");
            assert!(
                body.contains("id=\"root\""),
                "{path} should return the shell markup"
            );
        }
    }

    /// The fallback is scoped, not a catch-all. A missing asset answered with
    /// HTML would be parsed by the browser as JavaScript or CSS.
    #[tokio::test]
    async fn missing_assets_and_unknown_api_paths_stay_404() {
        let dir = dashboard();
        for path in [
            "/api/v1/nope",
            "/assets/does-not-exist.js",
            "/favicon.ico",
            "/nested/path/styles.css",
        ] {
            let (status, body) = get(&dir, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path} should be a 404");
            assert!(
                !body.contains("id=\"root\""),
                "{path} must not return the shell"
            );
        }
    }

    #[tokio::test]
    async fn real_assets_are_still_served() {
        let dir = dashboard();
        let (status, body) = get(&dir, "/assets/app.js").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "console.log(1)");
    }

    /// A misconfigured THS_WEB_DIR should fail visibly rather than serving an
    /// empty 200 that looks like a working dashboard.
    #[tokio::test]
    async fn a_missing_shell_is_reported_rather_than_served_empty() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (status, _) = get(&dir, "/wallet").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    fn wallet_status(
        state: &'static str,
        fully_scanned_height: Option<u64>,
        error: Option<&str>,
    ) -> WalletSyncStatus {
        WalletSyncStatus {
            state,
            fully_scanned_height,
            fully_scanned_hash: fully_scanned_height.map(|_| "canonical".to_owned()),
            observed_hash: fully_scanned_height.map(|_| "canonical".to_owned()),
            observed_height: fully_scanned_height,
            last_success_at: Some(123),
            error: error.map(str::to_owned),
        }
    }

    fn settlement_for_test(
        node: Option<u64>,
        lightwalletd: Option<u64>,
        wallet: &WalletSyncStatus,
    ) -> SettlementStatus {
        let checkpoint = |height| ChainCheckpoint {
            height,
            hash: "canonical".to_owned(),
        };
        settlement_status(
            node.map(checkpoint).as_ref(),
            lightwalletd.map(checkpoint).as_ref(),
            wallet,
        )
    }

    #[test]
    fn settlement_requires_all_three_components_at_the_same_tip() {
        let status = settlement_for_test(
            Some(105),
            Some(105),
            &wallet_status("ready", Some(105), None),
        );
        assert!(status.settled);
        assert_eq!(status.settlement_blocker, None);

        let status = settlement_for_test(
            Some(105),
            Some(104),
            &wallet_status("ready", Some(105), None),
        );
        assert!(!status.settled);
        assert_eq!(status.settlement_blocker, Some("height_mismatch"));

        let status = settlement_for_test(
            Some(105),
            Some(105),
            &wallet_status("ready", Some(106), None),
        );
        assert!(
            !status.settled,
            "a wallet ahead of the current tip needs reconciliation"
        );

        let canonical = ChainCheckpoint {
            height: 105,
            hash: "canonical".to_owned(),
        };
        let replacement = ChainCheckpoint {
            height: 105,
            hash: "replacement".to_owned(),
        };
        let wallet = wallet_status("ready", Some(105), None);
        let status = settlement_status(Some(&canonical), Some(&replacement), &wallet);
        assert_eq!(status.settlement_blocker, Some("chain_mismatch"));

        let status = settlement_status(Some(&replacement), Some(&replacement), &wallet);
        assert!(
            !status.settled,
            "a same-height reorg needs wallet reconciliation"
        );
        assert_eq!(status.settlement_blocker, Some("wallet_behind"));
    }

    #[test]
    fn settlement_reports_the_first_blocking_component() {
        let cases = [
            (
                None,
                Some(105),
                wallet_status("ready", Some(105), None),
                "node_unavailable",
            ),
            (
                Some(105),
                None,
                wallet_status("ready", Some(105), None),
                "lightwalletd_unavailable",
            ),
            (
                Some(105),
                Some(105),
                wallet_status("error", Some(104), Some("scan failed")),
                "wallet_error",
            ),
            (
                Some(105),
                Some(105),
                wallet_status("syncing", Some(104), None),
                "wallet_syncing",
            ),
            (
                Some(105),
                Some(105),
                wallet_status("ready", None, None),
                "wallet_behind",
            ),
            (
                Some(105),
                Some(105),
                wallet_status("ready", Some(104), None),
                "wallet_behind",
            ),
        ];

        for (node, lightwalletd, wallet, expected) in cases {
            let status = settlement_for_test(node, lightwalletd, &wallet);
            assert!(!status.settled);
            assert_eq!(status.settlement_blocker, Some(expected));
        }
    }

    #[test]
    fn settlement_allows_a_canonical_tip_before_the_wallet_birthday() {
        let tip = ChainCheckpoint {
            height: u64::from(WALLET_BIRTHDAY_HEIGHT) - 1,
            hash: "canonical".to_owned(),
        };
        let mut wallet = wallet_status("ready", None, None);
        wallet.observed_height = Some(tip.height);
        wallet.observed_hash = Some(tip.hash.clone());
        assert!(settlement_status(Some(&tip), Some(&tip), &wallet).settled);
        wallet.observed_hash = Some("orphaned".to_owned());
        assert!(!settlement_status(Some(&tip), Some(&tip), &wallet).settled);
    }

    #[test]
    fn address_faucet_resolves_only_published_user_addresses() {
        let (state, _dir) = state_with_local_wallet();
        for id in 1..=USER_ACCOUNT_COUNT {
            let account = state.0.store.account(id).unwrap();
            assert_eq!(
                internal_faucet_destination(&state.0.store, &account.transparent_address).unwrap(),
                Some((id, "transparent"))
            );
            assert_eq!(
                internal_faucet_destination(&state.0.store, &account.unified_address).unwrap(),
                Some((id, "ironwood"))
            );
            assert_eq!(
                internal_faucet_destination(
                    &state.0.store,
                    &format!("{} ", account.transparent_address)
                )
                .unwrap(),
                None
            );
        }
        let treasury = state.0.store.account(TREASURY_ACCOUNT_ID).unwrap();
        for address in [
            treasury.transparent_address,
            treasury.unified_address,
            "unmatched".into(),
        ] {
            assert_eq!(
                internal_faucet_destination(&state.0.store, &address).unwrap(),
                None
            );
        }
    }

    #[test]
    fn address_faucet_resolver_propagates_store_errors() {
        let store = Store::open(":memory:").unwrap();
        assert!(internal_faucet_destination(&store, "unmatched").is_err());
    }

    fn address_faucet_activity(store: &Store) -> Activity {
        let row = store
            .claim_faucet(2, "transparent", 100_000_000, "adapter-test")
            .unwrap();
        let row = store
            .record_prepared(&row.id, "faucet-txid", b"prepared bytes", 140)
            .unwrap();
        store.mark_broadcast(&row.id, &row.txid).unwrap()
    }

    #[test]
    fn address_faucet_response_preserves_pending_and_confirmed_status() {
        let (state, _dir) = state_with_local_wallet();
        let address = state.0.store.account(2).unwrap().transparent_address;
        let row = address_faucet_activity(&state.0.store);
        let pending = faucet_address_response(address.clone(), row.clone()).unwrap();
        assert_eq!(pending.status, "broadcast");
        assert_eq!(pending.block_hash, None);
        let row = state
            .0
            .store
            .confirm(&row.id, &row.txid, "inclusion-hash")
            .unwrap();
        let value =
            serde_json::to_value(faucet_address_response(address.clone(), row.clone()).unwrap())
                .unwrap();
        assert_eq!(
            value,
            json!({"address": address, "amount_zatoshi": 100_000_000, "txid": "faucet-txid", "block_hash": "inclusion-hash", "status": "confirmed", "last_error": null})
        );
        for block_hash in [None, Some(String::new())] {
            let mut invalid = row.clone();
            invalid.block_hash = block_hash;
            assert!(faucet_address_response(address.clone(), invalid).is_err());
        }
    }

    struct RecordingAddressFaucetRuntime {
        internal_calls: std::sync::Mutex<Vec<(u8, String, u64, String)>>,
        external_calls: std::sync::Mutex<Vec<(String, u64)>>,
        activity: Activity,
        fail_internal: bool,
        fail_external: bool,
    }

    impl RecordingAddressFaucetRuntime {
        fn new(store: &Store) -> Self {
            let row = address_faucet_activity(store);
            Self {
                internal_calls: Default::default(),
                external_calls: Default::default(),
                activity: store.confirm(&row.id, &row.txid, "inclusion-hash").unwrap(),
                fail_internal: false,
                fail_external: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl AddressFaucetRuntime for RecordingAddressFaucetRuntime {
        async fn fund_internal(
            &self,
            id: u8,
            pool: &str,
            amount: u64,
            key: &str,
        ) -> anyhow::Result<Activity> {
            self.internal_calls
                .lock()
                .unwrap()
                .push((id, pool.into(), amount, key.into()));
            anyhow::ensure!(!self.fail_internal, "internal funding failed");
            Ok(self.activity.clone())
        }

        async fn fund_external(
            &self,
            req: FaucetAddressRequest,
        ) -> anyhow::Result<FaucetAddressResponse> {
            self.external_calls
                .lock()
                .unwrap()
                .push((req.address.clone(), req.amount_zatoshi));
            anyhow::ensure!(!self.fail_external, "external funding failed");
            Ok(FaucetAddressResponse {
                address: req.address,
                amount_zatoshi: req.amount_zatoshi,
                txid: "external-txid".into(),
                block_hash: Some("external-block".into()),
                status: "confirmed".into(),
                last_error: None,
            })
        }
    }

    #[tokio::test]
    async fn address_faucet_dispatches_internal_transparent_and_unified() {
        let (state, _dir) = state_with_local_wallet();
        let runtime = RecordingAddressFaucetRuntime::new(&state.0.store);
        let account = state.0.store.account(2).unwrap();
        for (address, pool) in [
            (account.transparent_address, "transparent"),
            (account.unified_address, "ironwood"),
        ] {
            let Json(response) = execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address: address.clone(),
                    amount_zatoshi: 100_000_000,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{}", error.message));
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"address": address, "amount_zatoshi": 100_000_000, "txid": "faucet-txid", "block_hash": "inclusion-hash", "status": "confirmed", "last_error": null})
            );
            let calls = runtime.internal_calls.lock().unwrap();
            let call = calls.last().unwrap();
            assert_eq!((call.0, call.1.as_str(), call.2), (2, pool, 100_000_000));
        }
        assert!(runtime.external_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn address_faucet_retries_preserve_the_client_key() {
        let (state, _dir) = state_with_local_wallet();
        let runtime = RecordingAddressFaucetRuntime::new(&state.0.store);
        let address = state.0.store.account(2).unwrap().transparent_address;
        for _ in 0..2 {
            let Json(_response) = execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address: address.clone(),
                    amount_zatoshi: 100_000_000,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{}", error.message));
        }
        let calls = runtime.internal_calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].3, "address-test-key");
        assert_eq!(calls[1].3, "address-test-key");
        assert!(calls.iter().all(|call| require_key(&call.3).is_ok()));
        assert!(runtime.external_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn address_faucet_validation_precedes_dispatch() {
        let (state, _dir) = state_with_local_wallet();
        let runtime = RecordingAddressFaucetRuntime::new(&state.0.store);
        let address = state.0.store.account(2).unwrap().transparent_address;
        for (address, amount) in [
            ("not-an-address".into(), 1),
            (address.clone(), 0),
            (address.clone(), 500_000_001),
        ] {
            let error = execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address,
                    amount_zatoshi: amount,
                },
            )
            .await
            .err()
            .unwrap();
            assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
        }
        assert!(runtime.internal_calls.lock().unwrap().is_empty());
        assert!(runtime.external_calls.lock().unwrap().is_empty());
        let Json(_response) = execute_address_faucet(
            &state.0.store,
            &runtime,
            FaucetAddressRequest {
                idempotency_key: "address-test-key".into(),
                address,
                amount_zatoshi: 500_000_000,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(runtime.internal_calls.lock().unwrap()[0].2, 500_000_000);
    }

    #[tokio::test]
    async fn address_faucet_dispatch_preserves_external_behavior() {
        let (state, _dir) = state_with_local_wallet();
        let mut runtime = RecordingAddressFaucetRuntime::new(&state.0.store);
        let treasury = state.0.store.account(TREASURY_ACCOUNT_ID).unwrap();
        for address in [
            treasury.transparent_address.clone(),
            treasury.unified_address,
        ] {
            let Json(response) = execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address: address.clone(),
                    amount_zatoshi: 100_000_000,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{}", error.message));
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"address": address, "amount_zatoshi": 100_000_000, "txid": "external-txid", "block_hash": "external-block", "status": "confirmed", "last_error": null})
            );
            assert_eq!(
                runtime.external_calls.lock().unwrap().last().unwrap(),
                &(address, 100_000_000)
            );
        }
        runtime.fail_external = true;
        assert!(
            execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address: treasury.transparent_address,
                    amount_zatoshi: 1
                }
            )
            .await
            .is_err()
        );
        assert!(runtime.internal_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn address_faucet_internal_errors_do_not_fall_back() {
        let (state, _dir) = state_with_local_wallet();
        let mut runtime = RecordingAddressFaucetRuntime::new(&state.0.store);
        let address = state.0.store.account(2).unwrap().transparent_address;
        runtime.fail_internal = true;
        assert!(
            execute_address_faucet(
                &state.0.store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address: address.clone(),
                    amount_zatoshi: 100_000_000
                }
            )
            .await
            .is_err()
        );
        runtime.fail_internal = false;
        runtime.activity.status = "broadcast".into();
        runtime.activity.block_hash = None;
        let Json(pending) = execute_address_faucet(
            &state.0.store,
            &runtime,
            FaucetAddressRequest {
                idempotency_key: "address-test-key".into(),
                address: address.clone(),
                amount_zatoshi: 100_000_000,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(pending.status, "broadcast");
        assert_eq!(pending.block_hash, None);
        assert_eq!(runtime.internal_calls.lock().unwrap().len(), 2);
        let broken_store = Store::open(":memory:").unwrap();
        assert!(
            execute_address_faucet(
                &broken_store,
                &runtime,
                FaucetAddressRequest {
                    idempotency_key: "address-test-key".into(),
                    address,
                    amount_zatoshi: 1
                }
            )
            .await
            .is_err()
        );
        assert_eq!(runtime.internal_calls.lock().unwrap().len(), 2);
        assert!(runtime.external_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mining_adapter_rejects_missing_empty_and_multiple_rpc_hashes() {
        for hashes in [json!([]), json!([""]), json!(["hash-one", "hash-two"])] {
            let calls = Arc::new(AtomicUsize::new(0));
            let rpc_calls = calls.clone();
            let rpc = Router::new().route("/", post(move |Json(request): Json<Value>| {
                let hashes = hashes.clone();
                let calls = rpc_calls.clone();
                async move {
                    assert_eq!(request["method"], "generate");
                    assert_eq!(request["params"], json!([1]));
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":hashes,"error":null}))
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(listener, rpc).await.unwrap();
            });
            let (mut state, _dir) = state_with_local_wallet();
            Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);
            let app = router(state);
            let (status, _) = mining_request(
                &app,
                "POST",
                "/api/v1/mining/jobs",
                json!({"blocks":2,"idempotency_key":"malformed-hashes"}),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
            let failed = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let (_, value) =
                        mining_request(&app, "GET", "/api/v1/mining/jobs", Value::Null).await;
                    if value["job"]["state"] == "failed" {
                        break value["job"].clone();
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(failed["completed_blocks"], 0);
            assert_eq!(failed["progress_uncertain"], true);
            assert!(
                failed["error"]
                    .as_str()
                    .unwrap()
                    .contains("exactly one nonempty")
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                mining_request(
                    &app,
                    "POST",
                    "/api/v1/mining/jobs",
                    json!({"blocks":2,"idempotency_key":"malformed-hashes"})
                )
                .await,
                (StatusCode::ACCEPTED, failed)
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        }
    }

    #[tokio::test]
    async fn mining_adapter_accepts_one_nonempty_rpc_hash_and_forwards_events() {
        let rpc = Router::new().route(
            "/",
            post(|Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "generate");
                assert_eq!(request["params"], json!([1]));
                Json(json!({"result":["one-hash"],"error":null,"id":request["id"]}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, rpc).await.unwrap();
        });
        let (mut state, _dir) = state_with_local_wallet();
        Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);
        let mut events = state.0.events.subscribe();
        let adapter = ManualMiningAdapter(state);
        assert_eq!(adapter.generate_one().await.unwrap(), "one-hash");
        adapter.notify("mining");
        assert_eq!(events.recv().await.unwrap(), "mining");
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    struct ManualMiningRuntime {
        permits: tokio::sync::Semaphore,
        sync_permits: tokio::sync::Semaphore,
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
        fail_sync: bool,
    }

    impl ManualMiningRuntime {
        fn blocked() -> Self {
            Self {
                permits: tokio::sync::Semaphore::new(0),
                sync_permits: tokio::sync::Semaphore::new(1),
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
                fail_sync: false,
            }
        }

        async fn wait_for_first_call(&self) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let notified = self.entered.notified();
                    if self.calls.load(Ordering::SeqCst) > 0 {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .unwrap();
        }

        fn release(&self, blocks: usize) {
            self.permits.add_permits(blocks);
        }
    }

    #[async_trait::async_trait]
    impl MiningRuntime for ManualMiningRuntime {
        async fn generate_one(&self) -> anyhow::Result<String> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.entered.notify_one();
            self.permits.acquire().await.unwrap().forget();
            Ok(format!("hash-{call}"))
        }

        async fn synchronize(&self, tip_hash: &str) -> anyhow::Result<()> {
            assert!(tip_hash.starts_with("hash-"));
            self.sync_permits.acquire().await.unwrap().forget();
            anyhow::ensure!(!self.fail_sync, "injected wallet sync failure");
            Ok(())
        }

        fn notify(&self, _topic: &'static str) {}
    }

    fn state_with_mining(
        runtime: Arc<dyn MiningRuntime>,
        capacity: usize,
    ) -> (AppState, tempfile::TempDir) {
        let (mut state, dir) = state_with_local_wallet();
        let inner = Arc::get_mut(&mut state.0).unwrap();
        inner.mining_runtime = Some(runtime);
        inner.mining = Arc::new(MiningCoordinator::with_capacity(capacity));
        (state, dir)
    }

    async fn mining_request(
        app: &Router,
        method: &str,
        path: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn completed_mining(app: &Router) -> Value {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (status, value) =
                    mining_request(app, "GET", "/api/v1/mining/jobs", Value::Null).await;
                assert_eq!(status, StatusCode::OK);
                if value["job"]["state"] == "completed" {
                    return value["job"].clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn mining_routes_validate_admit_replay_and_preserve_capacity_history() {
        let runtime = Arc::new(ManualMiningRuntime::blocked());
        let (state, _dir) = state_with_mining(runtime.clone(), 1);
        let app = router(state);
        assert_eq!(
            mining_request(&app, "GET", "/api/v1/mining/jobs", Value::Null).await,
            (StatusCode::OK, json!({"job":null}))
        );
        for body in [
            json!({"blocks":0,"idempotency_key":"valid-key"}),
            json!({"blocks":10001,"idempotency_key":"valid-key"}),
            json!({"blocks":1,"idempotency_key":"short"}),
            json!({"blocks":1,"idempotency_key":"space key"}),
            json!({"blocks":1,"idempotency_key":"nonasciié"}),
            json!({"blocks":1,"idempotency_key":"x".repeat(129)}),
            json!({"blocks":1,"idempotency_key":"control\n"}),
        ] {
            let (status, value) = mining_request(&app, "POST", "/api/v1/mining/jobs", body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(value["error"]["status"], 400);
            assert!(value["error"]["message"].as_str().unwrap().len() > 5);
        }
        let body = json!({"blocks":3,"idempotency_key":"valid-key"});
        let (status, first) =
            mining_request(&app, "POST", "/api/v1/mining/jobs", body.clone()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(first["requested_blocks"], 3);
        assert_eq!(first["completed_blocks"], 0);
        assert_eq!(first["state"], "mining");
        assert_eq!(first["error"], Value::Null);
        assert_eq!(first["progress_uncertain"], false);
        let id = first["id"].as_str().unwrap();
        assert_eq!(
            mining_request(
                &app,
                "GET",
                &format!("/api/v1/mining/jobs/{id}"),
                Value::Null
            )
            .await,
            (StatusCode::OK, first.clone())
        );
        assert_eq!(
            mining_request(&app, "POST", "/api/v1/mining/jobs", body.clone()).await,
            (StatusCode::ACCEPTED, first.clone())
        );
        for conflict in [
            json!({"blocks":2,"idempotency_key":"valid-key"}),
            json!({"blocks":3,"idempotency_key":"another-key"}),
        ] {
            let (status, error) =
                mining_request(&app, "POST", "/api/v1/mining/jobs", conflict).await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(error["error"]["status"], 409);
        }
        let (status, error) =
            mining_request(&app, "POST", "/api/v1/mine", json!({"blocks":1})).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["error"]["status"], 409);
        let (status, error) =
            mining_request(&app, "GET", "/api/v1/mining/jobs/unknown", Value::Null).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(error["error"]["status"], 404);
        runtime.release(3);
        let completed = completed_mining(&app).await;
        assert_eq!(completed["completed_blocks"], 3);
        assert_eq!(
            mining_request(&app, "POST", "/api/v1/mining/jobs", body).await,
            (StatusCode::ACCEPTED, completed.clone())
        );
        assert_eq!(
            mining_request(
                &app,
                "GET",
                &format!("/api/v1/mining/jobs/{id}"),
                Value::Null
            )
            .await,
            (StatusCode::OK, completed)
        );
        let (status, error) = mining_request(
            &app,
            "POST",
            "/api/v1/mining/jobs",
            json!({"blocks":1,"idempotency_key":"another-key"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error["error"]["status"], 503);
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("capacity")
        );
        let (status, error) =
            mining_request(&app, "POST", "/api/v1/mine", json!({"blocks":1})).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error["error"]["status"], 503);
        assert_eq!(runtime.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn mining_legacy_waits_for_sync_and_preserves_full_response() {
        let runtime = Arc::new(ManualMiningRuntime {
            sync_permits: tokio::sync::Semaphore::new(0),
            ..ManualMiningRuntime::blocked()
        });
        let (state, _dir) = state_with_mining(runtime.clone(), 2);
        let app = router(state);
        for blocks in [0, 10001] {
            assert_eq!(
                mining_request(&app, "POST", "/api/v1/mine", json!({"blocks":blocks}))
                    .await
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        let request_app = app.clone();
        let task = tokio::spawn(async move {
            mining_request(&request_app, "POST", "/api/v1/mine", json!({"blocks":3})).await
        });
        runtime.wait_for_first_call().await;
        assert!(!task.is_finished());
        runtime.release(3);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (_, value) =
                    mining_request(&app, "GET", "/api/v1/mining/jobs", Value::Null).await;
                if value["job"]["state"] == "syncing" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        runtime.sync_permits.add_permits(1);
        assert_eq!(
            task.await.unwrap(),
            (
                StatusCode::OK,
                json!({"blocks":3,"hashes":["hash-1","hash-2","hash-3"]})
            )
        );
    }

    #[tokio::test]
    async fn mining_legacy_sync_failure_uses_error_envelope() {
        let runtime = Arc::new(ManualMiningRuntime {
            fail_sync: true,
            ..ManualMiningRuntime::blocked()
        });
        runtime.release(1);
        let (state, _dir) = state_with_mining(runtime, 1);
        let (status, error) =
            mining_request(&router(state), "POST", "/api/v1/mine", json!({"blocks":1})).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error["error"]["status"], 500);
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("Blocks mined, but wallet synchronization failed")
        );
    }

    #[tokio::test]
    async fn mining_survives_http_disconnect_and_discarded_async_response() {
        let runtime = Arc::new(ManualMiningRuntime::blocked());
        let (state, _dir) = state_with_mining(runtime.clone(), 2);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });
        let url = base_url.clone();
        let client_task = tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{url}/api/v1/mine"))
                .json(&json!({"blocks":3}))
                .send()
                .await
        });
        runtime.wait_for_first_call().await;
        client_task.abort();
        assert!(client_task.await.unwrap_err().is_cancelled());
        runtime.release(3);
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let value: Value = client
                    .get(format!("{base_url}/api/v1/mining/jobs"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if value["job"]["state"] == "completed" {
                    assert_eq!(value["job"]["completed_blocks"], 3);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let body = json!({"blocks":1,"idempotency_key":"discarded-response"});
        drop(
            client
                .post(format!("{base_url}/api/v1/mining/jobs"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        );
        let replay = client
            .post(format!("{base_url}/api/v1/mining/jobs"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        let replay: Value = replay.json().await.unwrap();
        runtime.sync_permits.add_permits(1);
        runtime.release(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let value: Value = client
                    .get(format!(
                        "{base_url}/api/v1/mining/jobs/{}",
                        replay["id"].as_str().unwrap()
                    ))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if value["state"] == "completed" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(runtime.calls.load(Ordering::SeqCst), 4);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    fn state_with_local_wallet() -> (AppState, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path().join("server.db")).expect("open store");
        store.initialize().expect("initialize store");
        let wallet =
            RealWallet::open(dir.path(), &store.seed().expect("wallet seed")).expect("open wallet");
        let state = AppState::new(store, wallet, "http://127.0.0.1:1".into(), "test".into());
        state.mark_started();
        (state, dir)
    }

    /// a client that falls behind the event buffer must be told to refetch
    /// everything, since it cannot know which topics it missed.
    #[tokio::test]
    async fn a_lagging_event_stream_asks_the_client_to_resync() {
        use futures_core::Stream;

        let (state, _dir) = state_with_local_wallet();
        let response = router(state.clone())
            .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let mut body = response.into_body().into_data_stream();
        for _ in 0..=128 {
            notify(&state, "chain");
        }

        let mut received = String::new();
        while let Ok(Some(Ok(chunk))) = tokio::time::timeout(
            Duration::from_millis(200),
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_next(cx)),
        )
        .await
        {
            received.push_str(&String::from_utf8_lossy(&chunk));
        }
        assert!(received.contains("data: resync"), "{received}");
    }

    #[tokio::test]
    async fn status_counts_accounts_without_reading_the_seed() {
        let (state, dir) = state_with_local_wallet();
        rusqlite::Connection::open(dir.path().join("server.db"))
            .unwrap()
            .execute("UPDATE metadata SET value='not-hex' WHERE key='seed'", [])
            .unwrap();

        let response = router(state)
            .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["account_count"], 5);
    }

    #[tokio::test]
    async fn account_reads_return_the_snapshot_without_contacting_lightwalletd() {
        let (state, _dir) = state_with_local_wallet();
        {
            let mut snapshot = state.0.wallet_snapshot.write().await;
            snapshot.accounts[0].ironwood_zatoshi = 400_000_000;
        }
        let response = router(state)
            .oneshot(
                Request::get("/api/v1/accounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let accounts = value.as_array().expect("account array");
        assert_eq!(
            accounts
                .iter()
                .map(|a| a["id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5],
        );
        assert_eq!(accounts[0]["ironwood_zatoshi"], 400_000_000);
        let expected_keys = std::collections::BTreeSet::from([
            "id",
            "name",
            "unified_address",
            "transparent_address",
            "transparent_zatoshi",
            "ironwood_zatoshi",
            "unified_full_viewing_key",
        ]);
        for account in accounts {
            let keys = account
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(keys, expected_keys);
            assert!(
                account["unified_full_viewing_key"]
                    .as_str()
                    .is_some_and(|key| !key.is_empty())
            );
        }
    }

    #[tokio::test]
    async fn a_scan_behind_the_tip_publishes_only_while_it_is_canonical() {
        let checkpoint = |height, hash: &str| ChainCheckpoint {
            height,
            hash: hash.into(),
        };
        let (target, tip) = (checkpoint(10, "a"), checkpoint(12, "c"));
        let publishable = |scanned: Option<ChainCheckpoint>, canonical: &'static str| {
            let (target, tip) = (target.clone(), tip.clone());
            async move {
                scan_is_publishable(scanned.as_ref(), &target, &tip, |_| async move {
                    Ok(canonical.to_owned())
                })
                .await
                .unwrap()
            }
        };
        assert!(publishable(Some(checkpoint(12, "c")), "c").await);
        // mined past after reaching the target.
        assert!(publishable(Some(checkpoint(10, "a")), "a").await);
        assert!(publishable(Some(checkpoint(11, "b")), "b").await);
        // reorganized.
        assert!(!publishable(Some(checkpoint(10, "a")), "x").await);
        // short of the target.
        assert!(!publishable(Some(checkpoint(9, "z")), "z").await);
        assert!(!publishable(Some(checkpoint(12, "z")), "c").await);
        assert!(!publishable(Some(checkpoint(13, "d")), "c").await);
        assert!(!publishable(None, "c").await);
        // the tip dropped below the target.
        assert!(
            !scan_is_publishable(
                Some(&checkpoint(10, "a")),
                &checkpoint(12, "c"),
                &checkpoint(10, "a"),
                |_| async { unreachable!() },
            )
            .await
            .unwrap()
        );
        let below_birthday = checkpoint(u64::from(WALLET_BIRTHDAY_HEIGHT) - 1, "a");
        assert!(
            scan_is_publishable(None, &below_birthday, &below_birthday, |_| async {
                unreachable!()
            })
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn publication_keeps_the_last_snapshot_unless_the_scan_reached_its_target() {
        use zcash_client_backend::{
            data_api::{
                chain::ChainState,
                scanning::{ScanPriority, ScanRange},
            },
            proto::compact_formats::CompactBlock,
        };
        use zcash_primitives::block::BlockHash;

        let hash = |height: u64| BlockHash([height as u8; 32]).to_string();
        // the node's tip height and its block hash at each height.
        let node = Arc::new(Mutex::new((4_u64, (0..=6).map(hash).collect::<Vec<_>>())));
        let rpc_node = node.clone();
        let rpc = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let node = rpc_node.clone();
                async move {
                    let (tip, hashes) = node.lock().unwrap().clone();
                    let result = match request["method"].as_str().unwrap() {
                        "getblockchaininfo" => {
                            json!({"chain":"regtest","blocks":tip,"bestblockhash":hashes[tip as usize]})
                        }
                        "getblockhash" => {
                            json!(hashes[request["params"][0].as_u64().unwrap() as usize])
                        }
                        method => panic!("unexpected rpc {method}"),
                    };
                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result,"error":null}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
        let (mut state, _dir) = state_with_local_wallet();
        Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);

        let wallet = &state.0.wallet;
        wallet.update_chain_tip(4).await.unwrap();
        let blocks = (2..=4)
            .map(|height| CompactBlock {
                height,
                hash: vec![height as u8; 32],
                prev_hash: vec![(height - 1) as u8; 32],
                chain_metadata: Some(Default::default()),
                ..Default::default()
            })
            .collect();
        wallet
            .scan_batch(
                ScanRange::from_parts(2.into()..5.into(), ScanPriority::Historic),
                blocks,
                ChainState::empty(1.into(), BlockHash([1; 32])),
            )
            .await
            .unwrap();
        let target = |height| ChainCheckpoint {
            height,
            hash: hash(height),
        };

        // the scan reached its target and the tip moved on.
        node.lock().unwrap().0 = 6;
        state.refresh_wallet_snapshot(&target(4)).await.unwrap();
        let published = state.wallet_sync_status().await;
        assert_eq!(
            (published.fully_scanned_height, published.observed_height),
            (Some(4), Some(6))
        );
        state.0.wallet_snapshot.write().await.accounts[0].ironwood_zatoshi = 400_000_000;

        // short of the target, orphaned, and a tip that dropped below the target.
        for (target_height, tip, scanned_hash) in
            [(5, 6, hash(4)), (4, 6, "ee".repeat(32)), (6, 4, hash(4))]
        {
            {
                let mut node = node.lock().unwrap();
                node.0 = tip;
                node.1[4] = scanned_hash;
            }
            assert!(
                state
                    .refresh_wallet_snapshot(&target(target_height))
                    .await
                    .is_err()
            );
            let snapshot = state.0.wallet_snapshot.read().await;
            assert_eq!(snapshot.accounts[0].ironwood_zatoshi, 400_000_000);
            assert_eq!(
                (
                    snapshot.status.fully_scanned_height,
                    snapshot.status.observed_height,
                    snapshot.status.last_success_at,
                ),
                (
                    published.fully_scanned_height,
                    published.observed_height,
                    published.last_success_at,
                )
            );
        }
    }

    #[tokio::test]
    async fn synchronization_failure_preserves_the_last_good_snapshot() {
        let (state, _dir) = state_with_local_wallet();
        {
            let mut snapshot = state.0.wallet_snapshot.write().await;
            snapshot.accounts[0].ironwood_zatoshi = 400_000_000;
            snapshot.status.last_success_at = Some(123);
        }

        assert!(state.synchronize_wallet(Some(1)).await.is_err());

        let snapshot = state.0.wallet_snapshot.read().await;
        assert_eq!(snapshot.accounts[0].ironwood_zatoshi, 400_000_000);
        assert_eq!(snapshot.status.last_success_at, Some(123));
        assert_eq!(snapshot.status.state, "error");
        assert!(snapshot.status.error.is_some());
    }

    #[tokio::test]
    async fn send_rejects_invalid_pools_before_wallet_work() {
        let (state, _dir) = state_with_local_wallet();
        for (source_pool, destination_pool) in [("sapling", "ironwood"), ("ironwood", "sapling")] {
            let response = router(state.clone())
                .oneshot(
                    Request::post("/api/v1/send")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&json!({
                                "from_account": 1,
                                "to_account": 2,
                                "source_pool": source_pool,
                                "destination_pool": destination_pool,
                                "amount_zatoshi": 1,
                                "idempotency_key": "invalid-pool",
                            }))
                            .unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    enum LifecycleSubmission {
        Broadcast,
        Expired,
        Unavailable,
        LostBroadcast,
    }

    struct RecordingPaymentLifecycle {
        store: Store,
        submissions: Mutex<VecDeque<LifecycleSubmission>>,
        record_fails: AtomicBool,
        confirm: bool,
    }

    impl RecordingPaymentLifecycle {
        fn new(store: Store, submissions: impl IntoIterator<Item = LifecycleSubmission>) -> Self {
            Self {
                store,
                submissions: Mutex::new(submissions.into_iter().collect()),
                record_fails: AtomicBool::new(false),
                confirm: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl PaymentLifecycle for RecordingPaymentLifecycle {
        async fn submit_prepared(&self, activity: &Activity) -> anyhow::Result<PreparedSubmission> {
            match self
                .submissions
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected submission")
            {
                LifecycleSubmission::Broadcast => Ok(PreparedSubmission::Broadcast(
                    self.store.mark_broadcast(&activity.id, &activity.txid)?,
                )),
                LifecycleSubmission::Unavailable => {
                    Err(tonic::Status::unavailable("broadcast unavailable").into())
                }
                LifecycleSubmission::LostBroadcast => {
                    self.store.mark_broadcast(&activity.id, &activity.txid)?;
                    Err(tonic::Status::unavailable("broadcast response lost").into())
                }
                LifecycleSubmission::Expired => Ok(PreparedSubmission::Expired(
                    self.store.reset_for_retry(&activity.id, &activity.txid)?,
                )),
            }
        }

        fn record_preparation_error(&self, id: &str, error: &str) -> anyhow::Result<()> {
            self.store.record_error(id, "preparing", "", error)?;
            Ok(())
        }

        fn record_prepared(
            &self,
            id: &str,
            prepared: &PreparedPayment,
        ) -> anyhow::Result<Activity> {
            if self.record_fails.load(Ordering::SeqCst) {
                anyhow::bail!("recording failed");
            }
            self.store.record_prepared(
                id,
                &prepared.txid,
                &prepared.raw_transaction,
                prepared.expiry_height,
            )
        }

        async fn confirm_after_mining(&self, activity: Activity) -> anyhow::Result<Activity> {
            if self.confirm {
                self.store
                    .confirm(&activity.id, &activity.txid, "confirmed-block")
            } else {
                Ok(activity)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_funding_retries_the_original_key_and_signed_bytes_across_failure_windows() {
        for window in ["before-prepare", "after-prepare", "lost-broadcast"] {
            let store = Store::open(":memory:").unwrap();
            store.initialize().unwrap();
            let submissions = match window {
                "before-prepare" => vec![LifecycleSubmission::Broadcast],
                "after-prepare" => vec![
                    LifecycleSubmission::Unavailable,
                    LifecycleSubmission::Broadcast,
                ],
                _ => vec![
                    LifecycleSubmission::LostBroadcast,
                    LifecycleSubmission::Broadcast,
                ],
            };
            let runtime = RecordingPaymentLifecycle::new(store.clone(), submissions);
            let prepare_calls = AtomicUsize::new(0);
            crate::retry_startup(|| async {
                let pending = store.claim_faucet(
                    1,
                    "ironwood",
                    500_000_000,
                    "startup-account-1-ironwood-v1",
                )?;
                let activity = execute_payment(&runtime, pending, |_| async {
                    let attempt = prepare_calls.fetch_add(1, Ordering::SeqCst);
                    if window == "before-prepare" && attempt == 0 {
                        return Err(tonic::Status::unavailable(
                            "preparation dependency unavailable",
                        )
                        .into());
                    }
                    Ok(PreparedPayment {
                        txid: "original-startup-txid".into(),
                        raw_transaction: b"original signed payment".to_vec(),
                        expiry_height: 140,
                    })
                })
                .await?;
                assert_eq!(activity.status, "confirmed");
                Ok(())
            })
            .await
            .unwrap();
            let activity = store
                .activity_for_key("startup-account-1-ironwood-v1")
                .unwrap()
                .unwrap();
            assert_eq!(activity.status, "confirmed");
            assert_eq!(activity.txid, "original-startup-txid");
            assert_eq!(activity.to_account, Some(1));
            assert_eq!(activity.amount_zatoshi, 500_000_000);
            assert_eq!(store.activities(100).unwrap().len(), 1);
            assert_eq!(
                store.prepared_transaction(&activity.id).unwrap(),
                crate::db::PreparedTransaction {
                    raw_transaction: b"original signed payment".to_vec(),
                    expiry_height: 140
                }
            );
            assert_eq!(
                prepare_calls.load(Ordering::SeqCst),
                if window == "before-prepare" { 2 } else { 1 }
            );
        }
    }

    #[tokio::test]
    async fn startup_confirmation_preserves_fatal_mining_errors_and_normal_post_pending_behavior() {
        for mode in ["warmup", "internal", "malformed-block", "store"] {
            let (local, _dir) = state_with_local_wallet();
            let app = Router::new().route("/", post(move |Json(request): Json<Value>| async move {
                let result = match request["method"].as_str().unwrap() {
                    "getrawtransaction" if mode == "store" => json!({"txid":"original-startup-txid", "confirmations":1, "blockhash":"confirmed-block"}),
                    "getrawtransaction" => json!({"txid":"original-startup-txid", "confirmations":0}),
                    "generate" if mode == "warmup" => return Json(json!({"error":{"code":-28,"message":"warming up"},"result":null})),
                    "generate" if mode == "internal" => return Json(json!({"error":{"code":-32603,"message":"internal failure"},"result":null})),
                    "generate" => json!(["mined-block"]),
                    "getblock" => json!({}),
                    method => panic!("unexpected startup method {method}"),
                };
                Json(json!({"result":result,"error":null}))
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let state = AppState::new(
                local.0.store.clone(),
                local.0.wallet.clone(),
                format!("http://{}", listener.local_addr().unwrap()),
                "test".into(),
            );
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let pending = state
                .0
                .store
                .claim_faucet(1, "ironwood", 500_000_000, "startup-account-1-ironwood-v1")
                .unwrap();
            let pending = state
                .0
                .store
                .record_prepared(
                    &pending.id,
                    "original-startup-txid",
                    b"original signed bytes",
                    140,
                )
                .unwrap();
            let pending = state
                .0
                .store
                .mark_broadcast(&pending.id, &pending.txid)
                .unwrap();
            if mode == "store" {
                rusqlite::Connection::open(_dir.path().join("server.db"))
                    .unwrap()
                    .execute("DROP TABLE activity", [])
                    .unwrap();
            }
            let error = confirm_after_mining(&state, pending.clone())
                .await
                .unwrap_err();
            assert_eq!(
                crate::startup_retryable(&error),
                mode == "warmup",
                "{mode}: {error:#}"
            );
            if mode == "warmup" || mode == "internal" {
                assert_eq!(
                    error.downcast_ref::<RpcError>().unwrap().code,
                    if mode == "warmup" { -28 } else { -32603 }
                );
            }
            if mode == "store" {
                assert!(error.downcast_ref::<rusqlite::Error>().is_some());
            } else {
                let saved = state.0.store.prepared_transaction(&pending.id).unwrap();
                assert_eq!(saved.raw_transaction, b"original signed bytes");
                state.mark_started();
                let (status, response) = api_request(&state, "POST", "/api/v1/faucet", json!({"account_id":1,"pool":"ironwood","amount_zatoshi":500_000_000,"idempotency_key":"startup-account-1-ironwood-v1"})).await;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(response["status"], "broadcast");
                assert_eq!(response["txid"], "original-startup-txid");
            }
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        }
    }

    fn lifecycle_claim(store: &Store, key: &str) -> Activity {
        store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                key,
                None,
            )
            .unwrap()
    }

    fn confirmation_claim(store: &Store, key: &str) -> Activity {
        let claim = lifecycle_claim(store, key);
        let prepared = store
            .record_prepared(&claim.id, key, b"signed", 140)
            .unwrap();
        store.mark_broadcast(&prepared.id, key).unwrap()
    }

    #[tokio::test]
    async fn slow_internal_lookup_does_not_starve_external_faucet_dispatch() {
        let external_lookups = Arc::new(AtomicUsize::new(0));
        let lookup_count = external_lookups.clone();
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let lookup_count = lookup_count.clone();
                async move {
                    let txid = request["params"][0].as_str().unwrap();
                    if txid == "slow-internal-txid" {
                        tokio::time::sleep(PAYMENT_LOOKUP_TIMEOUT + Duration::from_secs(1)).await;
                        return Json(json!({
                            "jsonrpc":"2.0",
                            "id":request["id"],
                            "result":null,
                            "error":{"code":-5,"message":"transaction not found"}
                        }));
                    }
                    lookup_count.fetch_add(1, Ordering::SeqCst);
                    Json(json!({
                        "jsonrpc":"2.0",
                        "id":request["id"],
                        "result":{"txid":txid,"confirmations":1,"blockhash":"external-block"},
                        "error":null
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (mut state, _dir) = state_with_local_wallet();
        Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);

        let internal = lifecycle_claim(&state.0.store, "slow-internal");
        state
            .0
            .store
            .record_prepared(&internal.id, "slow-internal-txid", b"internal bytes", 0)
            .unwrap();
        let external = state
            .0
            .store
            .claim_address_faucet("tm-external", 12_000, "fast-external")
            .unwrap();
        state
            .0
            .store
            .record_address_prepared(&external.id, "fast-external-txid", b"external bytes", 0)
            .unwrap();

        let mut activity_cursor = None;
        let mut address_cursor = None;
        progress_payment_intents(
            &state,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            &mut activity_cursor,
            &mut address_cursor,
        )
        .await
        .unwrap();

        let progressed = state
            .0
            .store
            .address_faucet_for_key("fast-external")
            .unwrap()
            .unwrap();
        assert_eq!(progressed.status, "confirmed");
        assert!(external_lookups.load(Ordering::SeqCst) >= 1);
        let pending_internal = state
            .0
            .store
            .activity_for_key("slow-internal")
            .unwrap()
            .unwrap();
        assert_eq!(pending_internal.status, "prepared");
        assert!(pending_internal.last_error.is_some());

        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn payment_confirmation_loop_recovers_reopened_records_without_wallet_sync() {
        let (state, dir) = state_with_local_wallet();
        confirmation_claim(&state.0.store, "reopened-txid");
        drop(state);
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_calls = calls.clone();
        let rpc = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let calls = rpc_calls.clone();
            async move {
                assert_eq!(request["method"], "getrawtransaction");
                assert_eq!(request["params"][0], "reopened-txid");
                let response = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    json!({"result": null, "error": {"code": -5, "message": "not found"}, "id": request["id"]})
                } else {
                    json!({"result": {"txid": "reopened-txid", "confirmations": 1, "blockhash": "inclusion-block"}, "error": null, "id": request["id"]})
                };
                Json(response)
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
        let store = Store::open(dir.path().join("server.db")).unwrap();
        let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
        let state = AppState::new(store, wallet, rpc_url, "test".into());
        let mut updates = state.0.events.subscribe();
        // The production loop must progress even while synchronization owns
        // its guard, and must not require a new block or a payment POST.
        let _sync = state.0.wallet_sync.lock().await;
        let worker = tokio::spawn(payment_confirmation_loop(state.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(updates.recv().await.unwrap(), "wallet");
        })
        .await
        .unwrap();
        let confirmed = state
            .0
            .store
            .activity_for_key("reopened-txid")
            .unwrap()
            .unwrap();
        assert_eq!(confirmed.status, "confirmed");
        assert_eq!(confirmed.txid, "reopened-txid");
        assert_eq!(confirmed.block_hash.as_deref(), Some("inclusion-block"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn payment_confirmation_loop_survives_request_confirmation_cancellation() {
        let (state, dir) = state_with_local_wallet();
        let claim = confirmation_claim(&state.0.store, "cancelled-request-txid");
        let store = state.0.store.clone();
        drop(state);
        let (entered, lookup_started) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release_lookup, released) = tokio::sync::oneshot::channel();
        let released = Arc::new(Mutex::new(Some(released)));
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_calls = calls.clone();
        let rpc = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let entered = entered.clone();
            let released = released.clone();
            let calls = rpc_calls.clone();
            async move {
                assert_eq!(request["method"], "getrawtransaction");
                assert_eq!(request["params"][0], "cancelled-request-txid");
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let released = released.lock().unwrap().take().unwrap();
                    entered.lock().unwrap().take().unwrap().send(()).unwrap();
                    // Cancellation happens after the request-side lookup starts,
                    // before it can confirm, auto-mine, or notify subscribers.
                    tokio::time::timeout(Duration::from_secs(10), released)
                        .await
                        .unwrap()
                        .unwrap();
                }
                Json(json!({
                    "result": {"txid": "cancelled-request-txid", "confirmations": 1, "blockhash": "recovered-block"},
                    "error": null,
                    "id": request["id"]
                }))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
        let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
        let state = AppState::new(store, wallet, rpc_url, "test".into());
        let mut updates = state.0.events.subscribe();
        let request_state = state.clone();
        let request_claim = claim.clone();
        let request =
            tokio::spawn(async move { confirm_after_mining(&request_state, request_claim).await });
        let lookup_started = tokio::time::timeout(Duration::from_secs(5), lookup_started).await;
        request.abort();
        let request_result = request.await;
        let lookup_released = release_lookup.send(());
        let pending = state.0.store.activity_for_key("cancelled-request-txid");
        let signed = state.0.store.prepared_transaction(&claim.id);
        let no_request_update = updates.try_recv();
        // Request cancellation must not cancel server-owned work. Holding the
        // sync guard also proves recovery does not depend on wallet scanning.
        let _sync = state.0.wallet_sync.lock().await;
        let worker = tokio::spawn(payment_confirmation_loop(state.clone()));
        let update = tokio::time::timeout(Duration::from_secs(5), updates.recv()).await;
        worker.abort();
        let worker_result = worker.await;
        server.abort();
        let server_result = server.await;

        lookup_started.unwrap().unwrap();
        lookup_released.unwrap();
        assert!(request_result.unwrap_err().is_cancelled());
        let pending = pending.unwrap().unwrap();
        assert_eq!(pending.id, claim.id);
        assert_eq!(pending.txid, claim.txid);
        assert_eq!(pending.status, "broadcast");
        assert!(pending.block_hash.is_none());
        assert_eq!(signed.unwrap().raw_transaction, b"signed");
        assert!(matches!(
            no_request_update,
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(update.unwrap().unwrap(), "wallet");
        let confirmed = state
            .0
            .store
            .activity_for_key("cancelled-request-txid")
            .unwrap()
            .unwrap();
        assert_eq!(confirmed.id, claim.id);
        assert_eq!(confirmed.txid, claim.txid);
        assert_eq!(confirmed.status, "confirmed");
        assert_eq!(confirmed.block_hash.as_deref(), Some("recovered-block"));
        assert_eq!(
            state
                .0
                .store
                .prepared_transaction(&claim.id)
                .unwrap()
                .raw_transaction,
            b"signed"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(worker_result.unwrap_err().is_cancelled());
        assert!(server_result.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn payment_confirmation_pass_continues_after_unknown_or_unavailable_transactions() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        for key in ["missing-txid", "unavailable-txid", "included-txid"] {
            confirmation_claim(&store, key);
        }
        let changed = reconcile_unconfirmed_with(
            &store,
            &mut None,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            |txid| async move {
                match txid.as_str() {
                    "missing-txid" => Ok(None),
                    "unavailable-txid" => anyhow::bail!("node unavailable"),
                    "included-txid" => Ok(Some(json!({
                        "confirmations": 1, "blockhash": "canonical-block"
                    }))),
                    _ => unreachable!(),
                }
            },
        )
        .await
        .unwrap();
        assert!(changed);
        for key in ["missing-txid", "unavailable-txid"] {
            let activity = store.activity_for_key(key).unwrap().unwrap();
            assert_eq!(activity.status, "broadcast");
            assert_eq!(activity.txid, key);
            assert!(activity.block_hash.is_none());
            assert_eq!(
                store
                    .prepared_transaction(&activity.id)
                    .unwrap()
                    .raw_transaction,
                b"signed"
            );
        }
        let included = store.activity_for_key("included-txid").unwrap().unwrap();
        assert_eq!(included.status, "confirmed");
        assert_eq!(included.block_hash.as_deref(), Some("canonical-block"));
    }

    #[tokio::test(start_paused = true)]
    async fn payment_confirmation_pass_resumes_after_a_stalled_lookup_exhausts_budget() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let mut claims = [
            confirmation_claim(&store, "first-txid"),
            confirmation_claim(&store, "second-txid"),
        ];
        claims.sort_by(|left, right| left.id.cmp(&right.id));
        let mut cursor = None;
        let changed = reconcile_unconfirmed_with(
            &store,
            &mut cursor,
            Instant::now() + Duration::from_millis(100),
            |_| std::future::pending::<anyhow::Result<Option<Value>>>(),
        )
        .await
        .unwrap();
        assert!(!changed);
        assert_eq!(cursor.as_deref(), Some(claims[0].id.as_str()));
        let calls = std::sync::Mutex::new(Vec::new());
        reconcile_unconfirmed_with(
            &store,
            &mut cursor,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            |txid| {
                calls.lock().unwrap().push(txid);
                std::future::ready(Ok(None))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            [claims[1].txid.clone(), claims[0].txid.clone()]
        );
    }

    #[tokio::test]
    async fn payment_confirmation_cursor_survives_a_confirmed_record_leaving_the_set() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let mut claims = [
            confirmation_claim(&store, "alpha-txid"),
            confirmation_claim(&store, "beta-txid"),
            confirmation_claim(&store, "gamma-txid"),
        ];
        claims.sort_by(|left, right| left.id.cmp(&right.id));
        store
            .confirm(&claims[1].id, &claims[1].txid, "block")
            .unwrap();
        let mut cursor = Some(claims[1].id.clone());
        let calls = std::sync::Mutex::new(Vec::new());
        reconcile_unconfirmed_with(
            &store,
            &mut cursor,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            |txid| {
                calls.lock().unwrap().push(txid);
                std::future::ready(Ok(None))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            [claims[2].txid.clone(), claims[0].txid.clone()]
        );
    }

    #[tokio::test]
    async fn payment_confirmation_does_not_confirm_a_mempool_transaction_or_a_replacement() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = confirmation_claim(&store, "original-txid");
        let mut cursor = None;
        reconcile_unconfirmed_with(
            &store,
            &mut cursor,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            |_| std::future::ready(Ok(Some(json!({"confirmations": 0})))),
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .activity_for_key("original-txid")
                .unwrap()
                .unwrap()
                .status,
            "broadcast"
        );
        let changed = reconcile_unconfirmed_with(
            &store,
            &mut cursor,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            |_| {
                store.reset_for_retry(&claim.id, &claim.txid).unwrap();
                store
                    .record_prepared(&claim.id, "replacement-txid", b"replacement", 180)
                    .unwrap();
                std::future::ready(Ok(Some(
                    json!({"confirmations": 1, "blockhash": "old-block"}),
                )))
            },
        )
        .await
        .unwrap();
        assert!(!changed);
        let replacement = store.activity_for_key("original-txid").unwrap().unwrap();
        assert_eq!(replacement.txid, "replacement-txid");
        assert_eq!(replacement.status, "prepared");
        assert!(replacement.block_hash.is_none());
    }

    fn prepared_payment_result(txid: &str, expiry_height: u64) -> PreparedPayment {
        PreparedPayment {
            txid: txid.into(),
            raw_transaction: format!("signed {txid}").into_bytes(),
            expiry_height,
        }
    }

    #[tokio::test]
    async fn payment_lifecycle_returns_confirmed_replays_without_preparation() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "confirmed-replay");
        let prepared = store
            .record_prepared(&claim.id, "confirmed-txid", b"signed", 140)
            .unwrap();
        store.mark_broadcast(&prepared.id, &prepared.txid).unwrap();
        let confirmed = store
            .confirm(&prepared.id, &prepared.txid, "confirmed-block")
            .unwrap();
        let runtime = RecordingPaymentLifecycle::new(store, []);
        let prepare_calls = Arc::new(AtomicUsize::new(0));

        let result = execute_payment(&runtime, confirmed.clone(), {
            let prepare_calls = prepare_calls.clone();
            move |_| {
                prepare_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(prepared_payment_result("unused", 180)))
            }
        })
        .await
        .unwrap();

        assert_eq!(result.id, confirmed.id);
        assert_eq!(result.status, "confirmed");
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn payment_lifecycle_returns_failed_replays_without_preparation() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "failed-replay");
        let failed = store
            .fail(&claim.id, "insufficient spendable funds")
            .unwrap();
        let runtime = RecordingPaymentLifecycle::new(store, []);
        let result = execute_payment(&runtime, failed.clone(), |_| async {
            panic!("failed replay must not prepare another payment")
        })
        .await
        .unwrap();

        assert_eq!(result.id, failed.id);
        assert_eq!(result.status, "failed");
        assert_eq!(
            result.last_error.as_deref(),
            Some("insufficient spendable funds")
        );
    }

    #[tokio::test]
    async fn insufficient_send_is_terminal_for_same_key_but_a_new_key_can_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "insufficient-send");
        let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
        let state = AppState::new(
            store.clone(),
            wallet,
            "http://127.0.0.1:1".into(),
            "test".into(),
        );
        state.mark_started();

        let failure = || {
            std::future::ready(Err(anyhow::Error::new(PaymentError::InsufficientFunds {
                available: 0,
                required: 12_000,
            })))
        };
        assert!(
            execute_payment(&state, claim.clone(), |_| failure())
                .await
                .is_err()
        );
        let failed = store
            .activity_for_key("insufficient-send")
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert!(
            failed
                .last_error
                .as_deref()
                .unwrap()
                .contains("insufficient")
        );

        drop(state);
        drop(store);
        let reopened = Store::open(&path).unwrap();
        reopened.initialize().unwrap();
        let wallet = RealWallet::open(dir.path(), &reopened.seed().unwrap()).unwrap();
        let state = AppState::new(
            reopened.clone(),
            wallet,
            "http://127.0.0.1:1".into(),
            "test".into(),
        );
        state.mark_started();
        let replay = lifecycle_claim(&reopened, "insufficient-send");
        assert_eq!(replay.id, failed.id);
        assert_eq!(replay.status, "failed");
        let prepare_calls = AtomicUsize::new(0);
        let replayed = execute_payment(&state, replay, |_| {
            prepare_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(PreparedPayment {
                txid: "later-funded-txid".into(),
                raw_transaction: "later-funded-raw".into(),
                expiry_height: 100,
            }))
        })
        .await
        .unwrap();
        assert_eq!(replayed.status, "failed");
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);

        let new_key = lifecycle_claim(&reopened, "insufficient-send-retry");
        assert_ne!(new_key.id, failed.id);
        assert_eq!(new_key.status, "preparing");
    }

    #[tokio::test]
    async fn insufficient_send_policy_waits_for_wallet_journal_and_checks_server_transaction() {
        let (state, dir) = state_with_local_wallet();
        let claim = state
            .0
            .store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "policy-store-transaction",
                None,
            )
            .unwrap();
        assert!(can_fail_insufficient_send(&state, &claim.id).await);

        state
            .0
            .store
            .record_prepared(&claim.id, "server-txid", b"signed", 140)
            .unwrap();
        assert!(!can_fail_insufficient_send(&state, &claim.id).await);

        let wallet_claim = state
            .0
            .store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "policy-wallet-transaction",
                None,
            )
            .unwrap();
        rusqlite::Connection::open(dir.path().join("wallet.db"))
            .unwrap()
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![wallet_claim.id, "wallet-txid", b"signed", 140_u64],
            )
            .unwrap();
        assert!(!can_fail_insufficient_send(&state, &wallet_claim.id).await);
    }

    #[test]
    fn progression_errors_are_recorded_against_post_transition_state() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();

        let internal = lifecycle_claim(&store, "transition-error-internal");
        let original_internal = store
            .unresolved_activities()
            .unwrap()
            .into_iter()
            .find(|intent| intent.key == "transition-error-internal")
            .unwrap();
        store
            .record_prepared(&internal.id, "current-internal-txid", b"signed", 140)
            .unwrap();
        record_current_activity_error(&store, &original_internal.key, "broadcast unavailable")
            .unwrap();
        let current_internal = store
            .activity_for_key("transition-error-internal")
            .unwrap()
            .unwrap();
        assert_eq!(current_internal.status, "prepared");
        assert_eq!(current_internal.txid, "current-internal-txid");
        assert_eq!(
            current_internal.last_error.as_deref(),
            Some("broadcast unavailable")
        );

        let external = store
            .claim_address_faucet("tm-external", 12_000, "transition-error-external")
            .unwrap();
        let intent = store
            .unresolved_address_faucets()
            .unwrap()
            .into_iter()
            .find(|intent| intent.key == "transition-error-external")
            .unwrap();
        store
            .record_address_prepared(&external.id, "current-external-txid", b"signed", 140)
            .unwrap();
        record_current_address_error(&store, &intent.key, "broadcast unavailable").unwrap();
        let current_external = store
            .address_faucet_for_key("transition-error-external")
            .unwrap()
            .unwrap();
        assert_eq!(current_external.status, "prepared");
        assert_eq!(current_external.txid, "current-external-txid");
        assert_eq!(
            current_external.last_error.as_deref(),
            Some("broadcast unavailable")
        );
    }

    #[tokio::test]
    async fn payment_lifecycle_submits_prepared_replays_without_preparation() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "prepared-replay");
        let prepared = store
            .record_prepared(&claim.id, "prepared-txid", b"signed", 140)
            .unwrap();
        let runtime = RecordingPaymentLifecycle::new(store, [LifecycleSubmission::Broadcast]);
        let prepare_calls = Arc::new(AtomicUsize::new(0));

        let result = execute_payment(&runtime, prepared, {
            let prepare_calls = prepare_calls.clone();
            move |_| {
                prepare_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(prepared_payment_result("unused", 180)))
            }
        })
        .await
        .unwrap();

        assert_eq!(result.status, "confirmed");
        assert_eq!(result.txid, "prepared-txid");
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn payment_lifecycle_prepares_again_after_expiry() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "expired-replay");
        let prepared = store
            .record_prepared(&claim.id, "expired-txid", b"expired", 140)
            .unwrap();
        let runtime = RecordingPaymentLifecycle::new(
            store,
            [LifecycleSubmission::Expired, LifecycleSubmission::Broadcast],
        );
        let prepare_calls = Arc::new(AtomicUsize::new(0));

        let result = execute_payment(&runtime, prepared, {
            let prepare_calls = prepare_calls.clone();
            move |_| {
                prepare_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(prepared_payment_result("replacement-txid", 180)))
            }
        })
        .await
        .unwrap();

        assert_eq!(result.status, "confirmed");
        assert_eq!(result.txid, "replacement-txid");
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn payment_lifecycle_does_not_discard_after_recording_failure() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "recording-failure");
        let runtime = RecordingPaymentLifecycle::new(store.clone(), []);
        runtime.record_fails.store(true, Ordering::SeqCst);

        let result = execute_payment(&runtime, claim.clone(), |_| {
            std::future::ready(Ok(prepared_payment_result("durable-txid", 180)))
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            store
                .activity_for_key("recording-failure")
                .unwrap()
                .unwrap()
                .status,
            "preparing"
        );
    }

    #[tokio::test]
    async fn payment_lifecycle_returns_pending_after_confirmation_failure() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = lifecycle_claim(&store, "mining-failure");
        let prepared = store
            .record_prepared(&claim.id, "pending-txid", b"signed", 140)
            .unwrap();
        let mut runtime = RecordingPaymentLifecycle::new(store, [LifecycleSubmission::Broadcast]);
        runtime.confirm = false;

        let result = execute_payment(&runtime, prepared, |_| {
            std::future::ready(Ok(prepared_payment_result("unused", 180)))
        })
        .await
        .unwrap();

        assert_eq!(result.status, "broadcast");
        assert_eq!(result.txid, "pending-txid");
    }

    #[tokio::test]
    async fn failed_recovery_keeps_a_claim_with_a_wallet_journal() {
        let (state, dir) = state_with_local_wallet();
        let claim = state
            .0
            .store
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
        rusqlite::Connection::open(dir.path().join("wallet.db"))
            .unwrap()
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![claim.id, "txid", b"signed transaction", 140_u64],
            )
            .unwrap();

        assert!(
            execute_payment(&state, claim.clone(), |_| {
                std::future::ready(Err(anyhow::anyhow!("preparation failed")))
            })
            .await
            .is_err()
        );

        assert_eq!(
            state.0.store.activity_for_key("same").unwrap().unwrap().id,
            claim.id
        );
    }

    #[tokio::test]
    async fn failed_preparation_preserves_the_claim_and_records_the_error() {
        let (state, _dir) = state_with_local_wallet();
        let claim = state
            .0
            .store
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

        assert!(
            execute_payment(&state, claim.clone(), |_| {
                std::future::ready(Err(anyhow::anyhow!("preparation failed")))
            })
            .await
            .is_err()
        );
        let retained = state.0.store.activity_for_key("same").unwrap().unwrap();
        assert_eq!(retained.id, claim.id);
        assert_eq!(retained.status, "preparing");
        assert_eq!(retained.last_error.as_deref(), Some("preparation failed"));
    }

    #[test]
    fn copies_spent_output_address_and_value_onto_vin() {
        let mut tx = json!({
            "vin": [{"txid": "aa", "vout": 1}],
            "vout": []
        });
        let mut prev = std::collections::HashMap::new();
        prev.insert(
            "aa".into(),
            json!({
                "vout": [
                    {"n": 0, "valueZat": 1},
                    {
                        "n": 1,
                        "valueZat": 50_000_000,
                        "scriptPubKey": {"addresses": ["tmABC"]}
                    }
                ]
            }),
        );
        attach_transparent_prevouts(&mut tx, &prev);
        assert_eq!(tx["vin"][0]["valueZat"], 50_000_000);
        assert_eq!(tx["vin"][0]["scriptPubKey"]["addresses"][0], "tmABC");
    }

    #[test]
    fn leaves_coinbase_inputs_untouched() {
        let mut tx = json!({"vin": [{"coinbase": "00"}]});
        attach_transparent_prevouts(&mut tx, &Default::default());
        assert_eq!(tx["vin"][0]["coinbase"], "00");
        assert!(tx["vin"][0].get("valueZat").is_none());
    }

    #[test]
    fn confirmed_block_hash_requires_confirmations_and_blockhash() {
        assert_eq!(
            confirmed_block_hash(&json!({
                "txid": "abc",
                "confirmations": 1,
                "blockhash": "0".repeat(64)
            })),
            Some("0000000000000000000000000000000000000000000000000000000000000000")
        );
        assert_eq!(
            confirmed_block_hash(&json!({
                "txid": "abc",
                "confirmations": 0,
                "blockhash": "0".repeat(64)
            })),
            None
        );
        assert_eq!(
            confirmed_block_hash(&json!({"txid": "abc", "confirmations": 3})),
            None
        );
        assert_eq!(
            confirmed_block_hash(&json!({"txid": "abc", "blockhash": "0".repeat(64)})),
            None
        );
        assert_eq!(
            confirmed_block_hash(&json!({
                "txid": "abc",
                "confirmations": 1,
                "blockhash": ""
            })),
            None
        );
        assert_eq!(confirmed_block_hash(&json!({"txid": "abc"})), None);
    }

    #[test]
    fn stale_confirmation_does_not_confirm_a_replacement_transaction() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claimed = store
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
            .record_prepared(&claimed.id, "old-txid", b"old transaction", 140)
            .unwrap();
        let old = store.activity_for_key("same").unwrap().unwrap();
        store.reset_for_retry(&claimed.id, "old-txid").unwrap();
        store
            .record_prepared(&claimed.id, "new-txid", b"new transaction", 180)
            .unwrap();

        let current = apply_confirmation(
            &store,
            &old,
            &serde_json::json!({
                "confirmations": 1,
                "blockhash": "old-block",
            }),
        )
        .unwrap();

        assert_eq!(current.txid, "new-txid");
        assert_eq!(current.status, "prepared");
        assert_eq!(current.block_hash, None);
    }

    #[test]
    fn apply_confirmation_uses_node_blockhash_not_a_generate_hash() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let pending = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "issue-67",
                None,
            )
            .unwrap();
        let pending = store
            .record_prepared(&pending.id, "txid-abc", b"raw transaction", 140)
            .unwrap();
        let pending = store.mark_broadcast(&pending.id, &pending.txid).unwrap();
        assert_eq!(pending.status, "broadcast");

        let generate_hash = "generate-hash-that-must-not-be-stored";
        let mempool = json!({"txid": "txid-abc", "confirmations": 0});
        let still = apply_confirmation(&store, &pending, &mempool).unwrap();
        assert_eq!(still.status, "broadcast");
        assert_eq!(still.block_hash, None);

        let mined = json!({
            "txid": "txid-abc",
            "confirmations": 1,
            "blockhash": "b".repeat(64)
        });
        let confirmed = apply_confirmation(&store, &pending, &mined).unwrap();
        let expected_hash = "b".repeat(64);
        assert_eq!(confirmed.status, "confirmed");
        assert_eq!(
            confirmed.block_hash.as_deref(),
            Some(expected_hash.as_str())
        );
        assert_ne!(confirmed.block_hash.as_deref(), Some(generate_hash));
        assert_eq!(confirmed.txid, "txid-abc");
    }

    fn text_memo_param(text: &str) -> String {
        zip321::memo_to_base64(&MemoBytes::from(&text.parse::<Memo>().unwrap()))
    }

    /// A unified address from a seed unrelated to the development accounts.
    fn foreign_unified_address<P: zcash_protocol::consensus::Parameters>(params: &P) -> String {
        use zcash_keys::keys::{UnifiedAddressRequest, UnifiedSpendingKey};
        let usk =
            UnifiedSpendingKey::from_seed(params, &[7u8; 32], zip32::AccountId::ZERO).unwrap();
        let (address, _) = usk
            .to_unified_full_viewing_key()
            .default_address(UnifiedAddressRequest::AllAvailableKeys)
            .unwrap();
        address.encode(params)
    }

    #[test]
    fn payment_uri_resolves_a_development_account_with_amount_and_memo() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let account = store.account(2).unwrap();
        let uri = format!(
            "zcash:{}?amount=1.5&memo={}",
            account.unified_address,
            text_memo_param("rent for October")
        );

        let parsed = payment_uri(&store, &uri).unwrap();
        assert_eq!(parsed.address, account.unified_address);
        assert_eq!(parsed.destination_pool, "ironwood");
        assert_eq!(parsed.to_account, Some(2));
        assert_eq!(parsed.amount_zatoshi, Some(150_000_000));
        assert_eq!(parsed.memo.as_deref(), Some("rent for October"));
    }

    #[test]
    fn payment_uri_accepts_transparent_and_external_regtest_addresses() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let transparent = store.account(3).unwrap().transparent_address;
        let parsed = payment_uri(&store, &format!("zcash:?address={transparent}")).unwrap();
        assert_eq!(parsed.destination_pool, "transparent");
        assert_eq!(parsed.to_account, Some(3));
        assert_eq!(parsed.amount_zatoshi, None);

        let external = foreign_unified_address(&regtest_network());
        let parsed = payment_uri(&store, &format!("zcash:{external}?amount=0.1")).unwrap();
        assert_eq!(parsed.to_account, None);
        assert_eq!(parsed.address, external);
    }

    #[test]
    fn payment_uri_rejects_unsupported_requests() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let ua = store.account(1).unwrap().unified_address;
        let taddr = store.account(2).unwrap().transparent_address;
        let mainnet = foreign_unified_address(&zcash_protocol::consensus::MainNetwork);
        let rejected = [
            format!("zcash:{taddr}?amount=1&memo={}", text_memo_param("hi")),
            format!("zcash:?address={ua}&amount=1&address.1={taddr}&amount.1=2"),
            format!("zcash:{mainnet}?amount=1"),
            format!("zcash:{ua}?amount=1&req-unknown=x"),
            format!("zcash:{ua}?amount=0.000000001"),
            "not a uri".to_owned(),
        ];
        for uri in rejected {
            let error = payment_uri(&store, &uri).expect_err(&uri);
            assert_eq!(error.status, StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[test]
    fn send_addresses_map_to_their_pool() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let account = store.account(1).unwrap();
        assert_eq!(
            require_send_address(&account.unified_address).ok(),
            Some("ironwood")
        );
        assert_eq!(
            require_send_address(&account.transparent_address).ok(),
            Some("transparent")
        );
        let mainnet = foreign_unified_address(&zcash_protocol::consensus::MainNetwork);
        assert!(require_send_address(&mainnet).is_err());
        assert!(require_send_address("not-an-address").is_err());
    }

    #[test]
    fn send_validation_requires_a_matching_pool_for_an_address() {
        let external = foreign_unified_address(&regtest_network());
        let request = |destination_pool: &str| {
            let mut body = valid_send();
            body.as_object_mut().unwrap().remove("to_account");
            body["to_address"] = json!(external);
            body["destination_pool"] = json!(destination_pool);
            serde_json::from_value::<SendRequest>(body).unwrap()
        };
        let req = request("ironwood");
        assert!(matches!(
            validate_send(&req),
            Ok((Recipient::Address(address), None)) if address == external
        ));
        let req = request("transparent");
        assert_eq!(
            validate_send(&req).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );

        let mut both = valid_send();
        both["to_address"] = json!(external);
        let req = serde_json::from_value::<SendRequest>(both).unwrap();
        assert_eq!(
            validate_send(&req).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn reserves_the_treasury_account_from_public_operations() {
        for id in 1..=USER_ACCOUNT_COUNT {
            assert!(require_user_account(id).is_ok());
        }
        assert!(require_user_account(TREASURY_ACCOUNT_ID).is_err());
    }

    fn is_bad_request<T>(result: ApiResult<T>) -> bool {
        matches!(result, Err(error) if error.status == StatusCode::BAD_REQUEST)
    }

    #[test]
    fn copies_the_ironwood_root_from_the_treestate() {
        let mut block = json!({"hash": "ab", "finalorchardroot": "orchard-root"});
        add_ironwood_root(
            &mut block,
            &json!({"ironwood": {"commitments": {"finalRoot": "ironwood-root", "finalState": "00"}}}),
        );
        assert_eq!(block["finalironwoodroot"], "ironwood-root");
        assert_eq!(block["finalorchardroot"], "orchard-root");

        // At the genesis block Zakura returns empty Ironwood commitments.
        let mut block = json!({"hash": "ab"});
        add_ironwood_root(&mut block, &json!({"ironwood": {"commitments": {}}}));
        assert!(block.get("finalironwoodroot").is_none());
    }

    #[test]
    fn rejects_orchard_as_a_pool_after_nu6_3() {
        assert!(require_pool("ironwood", "pool").is_ok());
        assert!(require_pool("transparent", "pool").is_ok());
        let Err(error) = require_pool("orchard", "destination_pool") else {
            panic!("orchard must be rejected");
        };
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("use ironwood"));
        assert!(is_bad_request(require_pool("sapling", "pool")));
    }

    #[test]
    fn absent_and_null_memos_differ_from_an_explicit_empty_memo() {
        let request = |memo: Option<Value>| {
            let mut body = json!({
                "from_account": 1, "to_account": 2, "source_pool": "ironwood",
                "destination_pool": "ironwood", "amount_zatoshi": 1,
                "idempotency_key": "memo-test-key",
            });
            if let Some(memo) = memo {
                body["memo"] = memo;
            }
            serde_json::from_value::<SendRequest>(body).expect("valid request shape")
        };
        assert_eq!(request(None).memo, None);
        assert_eq!(request(Some(Value::Null)).memo, None);
        assert_eq!(request(Some(json!(""))).memo.as_deref(), Some(""));

        assert!(matches!(parse_memo(None, "ironwood"), Ok(None)));
        assert!(matches!(parse_memo(None, "transparent"), Ok(None)));
        let Ok(Some(empty)) = parse_memo(Some(""), "ironwood") else {
            panic!("an explicit empty memo must be kept");
        };
        assert_eq!(empty.as_array(), &[0u8; 512]);
    }

    #[test]
    fn any_present_memo_is_rejected_for_a_transparent_destination() {
        assert!(is_bad_request(parse_memo(Some("hi"), "transparent")));
        assert!(is_bad_request(parse_memo(Some(""), "transparent")));
    }

    #[test]
    fn memos_are_bounded_by_utf8_bytes() {
        let Ok(Some(memo)) = parse_memo(Some("thanks for lunch"), "ironwood") else {
            panic!("expected an encoded memo");
        };
        assert_eq!(&memo.as_slice()[..16], b"thanks for lunch");
        assert!(memo.as_slice()[16..].iter().all(|byte| *byte == 0));

        assert!(parse_memo(Some(&"a".repeat(512)), "ironwood").is_ok());
        assert!(is_bad_request(parse_memo(
            Some(&"a".repeat(513)),
            "ironwood"
        )));

        // Three bytes per character: 170 fit (510 bytes), 171 do not (513).
        let Ok(Some(cjk)) = parse_memo(Some(&"桜".repeat(170)), "ironwood") else {
            panic!("510 bytes of multibyte text must fit");
        };
        assert_eq!(&cjk.as_slice()[..510], "桜".repeat(170).as_bytes());
        assert!(is_bad_request(parse_memo(
            Some(&"桜".repeat(171)),
            "ironwood"
        )));

        // Four bytes per character: exactly 512 fits, one more does not.
        assert!(parse_memo(Some(&"🌸".repeat(128)), "ironwood").is_ok());
        assert!(is_bad_request(parse_memo(
            Some(&"🌸".repeat(129)),
            "ironwood"
        )));
    }

    #[test]
    fn memos_must_not_end_with_nul() {
        assert!(is_bad_request(parse_memo(Some("hi\0"), "ironwood")));
        assert!(is_bad_request(parse_memo(Some("\0"), "ironwood")));
        assert!(parse_memo(Some("a\0b"), "ironwood").is_ok());
    }

    #[tokio::test]
    async fn payment_request_endpoint_round_trips_an_exact_amount_and_memo() {
        let (state, _dir) = state_with_local_wallet();
        let account = state.0.store.account(3).unwrap();
        let memo = "rent for October 🌸";
        let response = router(state)
            .oneshot(
                Request::post("/api/v1/zip321/request")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "account_id": account.id,
                            "amount_zatoshi": 123_456_789,
                            "memo": memo,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let uri = body["uri"].as_str().unwrap();
        assert!(uri.starts_with("zcash:uregtest1"));
        let parsed = zip321::TransactionRequest::from_uri(uri).unwrap();
        let payment = parsed.payments().values().next().unwrap();
        assert_eq!(parsed.payments().len(), 1);
        assert_eq!(
            payment.recipient_address().encode(),
            account.unified_address
        );
        assert_eq!(payment.amount().map(u64::from), Some(123_456_789));
        let encoded_memo = payment.memo().unwrap().as_slice();
        assert_eq!(&encoded_memo[..memo.len()], memo.as_bytes());
        assert!(encoded_memo[memo.len()..].iter().all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn payment_request_endpoint_omits_an_optional_amount_and_memo() {
        let (state, _dir) = state_with_local_wallet();
        let account = state.0.store.account(5).unwrap();
        let response = router(state)
            .oneshot(
                Request::post("/api/v1/zip321/request")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"account_id": account.id}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body["uri"].as_str(),
            Some(format!("zcash:{}", account.unified_address).as_str())
        );
    }

    #[tokio::test]
    async fn payment_request_endpoint_rejects_invalid_accounts_amounts_and_memos() {
        let (state, _dir) = state_with_local_wallet();
        let cases = [
            json!({"account_id": 0}),
            json!({"account_id": TREASURY_ACCOUNT_ID}),
            json!({"account_id": 1, "amount_zatoshi": 0}),
            json!({"account_id": 1, "amount_zatoshi": MAX_MONEY + 1}),
            json!({"account_id": 1, "memo": "a".repeat(513)}),
            json!({"account_id": 1, "memo": "memo\0"}),
        ];

        for body in cases {
            let response = router(state.clone())
                .oneshot(
                    Request::post("/api/v1/zip321/request")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        }
    }

    const REPLAY_KEY: &str = "existing-idempotency-key";

    /// A state with a real store and offline wallet whose node RPC refuses
    /// connections, so any synchronization or network access surfaces as a
    /// 500 rather than the 400 or replay these tests expect.
    fn offline_state() -> (AppState, tempfile::TempDir) {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
        let state = AppState::new(store, wallet, "http://127.0.0.1:1".into(), "test".into());
        state.mark_started();
        (state, dir)
    }

    fn valid_send() -> Value {
        json!({
            "from_account": 1,
            "to_account": 2,
            "source_pool": "ironwood",
            "destination_pool": "ironwood",
            "amount_zatoshi": 100_000,
            "idempotency_key": REPLAY_KEY,
        })
    }

    async fn post_send(state: &AppState, body: &Value) -> (StatusCode, Value) {
        let response = router(state.clone())
            .oneshot(
                Request::post("/api/v1/send")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn activity_ids(state: &AppState) -> Vec<String> {
        state
            .0
            .store
            .activities(100)
            .unwrap()
            .into_iter()
            .map(|activity| activity.id)
            .collect()
    }

    #[test]
    fn send_validation_allows_same_account_only_between_pools() {
        for to in [1, 2] {
            for source in ["ironwood", "transparent"] {
                for destination in ["ironwood", "transparent"] {
                    let mut body = valid_send();
                    body["to_account"] = json!(to);
                    body["source_pool"] = json!(source);
                    body["destination_pool"] = json!(destination);
                    let req = serde_json::from_value::<SendRequest>(body).unwrap();
                    let result = validate_send(&req);
                    if to == 1 && source == destination {
                        let error = result.unwrap_err();
                        assert_eq!(error.status, StatusCode::BAD_REQUEST);
                        assert_eq!(
                            error.message,
                            "choose a different account or a different destination pool"
                        );
                    } else {
                        assert!(
                            matches!(result, Ok((Recipient::Account(id), None)) if id == to),
                            "{to}: {source} -> {destination}"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn invalid_sends_are_rejected_before_replay_or_synchronization() {
        let (state, _dir) = offline_state();
        let original = state
            .0
            .store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                100_000,
                REPLAY_KEY,
                None,
            )
            .unwrap();
        let before = activity_ids(&state);

        let with = |fields: &[(&str, Value)]| {
            let mut body = valid_send();
            for (field, value) in fields {
                body[*field] = value.clone();
            }
            body
        };
        let transparent = ("destination_pool", json!("transparent"));
        let cases = [
            ("short key", with(&[("idempotency_key", json!("short"))])),
            (
                "key with spaces",
                with(&[("idempotency_key", json!("has spaces here"))]),
            ),
            ("account 0", with(&[("from_account", json!(0))])),
            (
                "treasury account",
                with(&[("to_account", json!(TREASURY_ACCOUNT_ID))]),
            ),
            ("same account and pool", with(&[("to_account", json!(1))])),
            (
                "same account and transparent pool",
                with(&[
                    ("to_account", json!(1)),
                    ("source_pool", json!("transparent")),
                    ("destination_pool", json!("transparent")),
                ]),
            ),
            (
                "same account and unsupported source",
                with(&[("to_account", json!(1)), ("source_pool", json!("sapling"))]),
            ),
            (
                "same account and unsupported destination",
                with(&[
                    ("to_account", json!(1)),
                    ("destination_pool", json!("orchard")),
                ]),
            ),
            (
                "same account unshield with memo",
                with(&[
                    ("to_account", json!(1)),
                    ("destination_pool", json!("transparent")),
                    ("memo", json!("hi")),
                ]),
            ),
            (
                "bad source pool",
                with(&[("source_pool", json!("sapling"))]),
            ),
            (
                "bad destination pool",
                with(&[("destination_pool", json!("sprout"))]),
            ),
            ("zero amount", with(&[("amount_zatoshi", json!(0))])),
            (
                "amount over MAX_MONEY",
                with(&[("amount_zatoshi", json!(MAX_MONEY + 1))]),
            ),
            ("memo too long", with(&[("memo", json!("a".repeat(513)))])),
            ("memo ending in NUL", with(&[("memo", json!("hi\u{0}"))])),
            (
                "memo to transparent",
                with(&[transparent.clone(), ("memo", json!("hi"))]),
            ),
            (
                "empty memo to transparent",
                with(&[transparent, ("memo", json!(""))]),
            ),
        ];
        for (name, body) in cases {
            let (status, response) = post_send(&state, &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {response}");
            assert_eq!(activity_ids(&state), before, "{name} changed activity");
        }
        assert_eq!(
            state
                .0
                .store
                .activity_for_key(REPLAY_KEY)
                .unwrap()
                .unwrap()
                .id,
            original.id
        );
    }

    #[tokio::test]
    async fn same_account_cross_pool_replays_preserve_activity_and_identity() {
        for (source, destination) in [("ironwood", "transparent"), ("transparent", "ironwood")] {
            let (state, _dir) = offline_state();
            let original = state
                .0
                .store
                .claim_transfer(
                    1,
                    Recipient::Account(1),
                    source,
                    destination,
                    100_000,
                    REPLAY_KEY,
                    None,
                )
                .unwrap();
            state
                .0
                .store
                .record_prepared(&original.id, "txid", b"signed transaction", 0)
                .unwrap();
            state.0.store.mark_broadcast(&original.id, "txid").unwrap();
            state
                .0
                .store
                .confirm(&original.id, "txid", &"c".repeat(64))
                .unwrap();
            let before = activity_ids(&state);
            let mut body = valid_send();
            body["to_account"] = json!(1);
            body["source_pool"] = json!(source);
            body["destination_pool"] = json!(destination);
            for _ in 0..2 {
                let (status, response) = post_send(&state, &body).await;
                assert_eq!(status, StatusCode::OK, "{response}");
                assert_eq!(response["id"], original.id);
                assert_eq!(response["txid"], "txid");
                assert_eq!(response["from_account"], 1);
                assert_eq!(response["to_account"], 1);
                assert_eq!(response["source_pool"], source);
                assert_eq!(response["destination_pool"], destination);
            }
            for changed in [("amount_zatoshi", json!(100_001)), ("to_account", json!(2))] {
                let mut conflict = body.clone();
                conflict[changed.0] = changed.1;
                assert_eq!(post_send(&state, &conflict).await.0, StatusCode::CONFLICT);
            }
            let mut reverse = body.clone();
            reverse["source_pool"] = json!(destination);
            reverse["destination_pool"] = json!(source);
            assert_eq!(post_send(&state, &reverse).await.0, StatusCode::CONFLICT);
            assert_eq!(activity_ids(&state), before);
        }
    }

    #[tokio::test]
    async fn a_valid_replay_returns_the_original_without_the_wallet_or_network() {
        let (state, _dir) = offline_state();
        let original = state
            .0
            .store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                100_000,
                REPLAY_KEY,
                None,
            )
            .unwrap();
        state
            .0
            .store
            .record_prepared(&original.id, "txid", b"signed transaction", 0)
            .unwrap();
        state.0.store.mark_broadcast(&original.id, "txid").unwrap();
        state
            .0
            .store
            .confirm(&original.id, "txid", &"c".repeat(64))
            .unwrap();
        let before = activity_ids(&state);

        for memo in [None, Some(Value::Null)] {
            let mut body = valid_send();
            if let Some(memo) = memo {
                body["memo"] = memo;
            }
            let (status, response) = post_send(&state, &body).await;
            assert_eq!(status, StatusCode::OK, "{response}");
            assert_eq!(response["id"], original.id);
            assert_eq!(response["txid"], "txid");
        }
        for memo in ["", "different memo"] {
            let mut body = valid_send();
            body["memo"] = json!(memo);
            let (status, _) = post_send(&state, &body).await;
            assert_eq!(status, StatusCode::CONFLICT);
        }
        assert_eq!(activity_ids(&state), before);

        // Guard for the harness itself: a send that is not a replay must reach
        // the (unreachable) network and fail, so the 200s above prove none did.
        let mut fresh = valid_send();
        fresh["idempotency_key"] = json!("a-key-never-used-before");
        let (status, _) = post_send(&state, &fresh).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let after = activity_ids(&state);
        assert_eq!(after.len(), before.len() + 1);
        assert!(before.iter().all(|id| after.contains(id)));
        let retained = state
            .0
            .store
            .activity_for_key("a-key-never-used-before")
            .unwrap()
            .unwrap();
        assert_eq!(retained.status, "preparing");
        assert!(retained.last_error.is_some());
    }

    #[test]
    fn faucet_accepts_only_regtest_unified_and_transparent_addresses() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let account = store.account(1).unwrap();

        assert!(require_faucet_address(&account.unified_address).is_ok());
        assert!(require_faucet_address(&account.transparent_address).is_ok());
        assert!(require_faucet_address("not-an-address").is_err());
    }

    #[tokio::test]
    async fn address_faucet_rejects_missing_or_invalid_operation_keys_before_network_work() {
        let (state, _dir) = state_with_local_wallet();
        let address = state.0.store.account(1).unwrap().unified_address;
        for key in [None, Some("short"), Some("has spaces")] {
            let mut body = json!({"address": address, "amount_zatoshi": 1});
            if let Some(key) = key {
                body["idempotency_key"] = json!(key);
            }
            let response = router(state.clone())
                .oneshot(
                    Request::post("/api/v1/faucet/address")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn internal_address_faucet_replays_preserve_activity_after_store_reopen() {
        for pool in ["transparent", "ironwood"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("app.db");
            let store = Store::open(&path).unwrap();
            store.initialize().unwrap();
            let account = store.account(2).unwrap();
            let address = if pool == "transparent" {
                account.transparent_address
            } else {
                account.unified_address
            };
            let original = store.claim_faucet(2, pool, 123, "internal-replay").unwrap();
            store
                .record_prepared(&original.id, "original-txid", b"signed bytes", 140)
                .unwrap();
            store
                .confirm(&original.id, "original-txid", "original-block")
                .unwrap();
            drop(store);

            let store = Store::open(&path).unwrap();
            store.initialize().unwrap();
            let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
            let state = AppState::new(store, wallet, "http://127.0.0.1:1".into(), "test".into());
            state.mark_started();
            let body = json!({"address": address, "amount_zatoshi": 123, "idempotency_key": "internal-replay"});
            for _ in 0..2 {
                let response = router(state.clone())
                    .oneshot(
                        Request::post("/api/v1/faucet/address")
                            .header("content-type", "application/json")
                            .body(Body::from(body.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let result: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(result["txid"], "original-txid");
                assert_eq!(result["block_hash"], "original-block");
                assert_eq!(result["status"], "confirmed");
            }
            for (field, value) in [
                ("amount_zatoshi", json!(124)),
                (
                    "address",
                    json!(state.0.store.account(3).unwrap().transparent_address),
                ),
            ] {
                let mut conflict = body.clone();
                conflict[field] = value;
                let response = router(state.clone())
                    .oneshot(
                        Request::post("/api/v1/faucet/address")
                            .header("content-type", "application/json")
                            .body(Body::from(conflict.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::CONFLICT);
            }
            let activities = state.0.store.activities(100).unwrap();
            assert_eq!(activities.len(), 1);
            assert_eq!(activities[0].id, original.id);
            assert_eq!(activities[0].txid, "original-txid");
        }
    }

    #[tokio::test]
    async fn confirmed_address_faucet_replays_without_sending_a_second_payment() {
        let (state, _dir) = state_with_local_wallet();
        let address = state.0.store.account(1).unwrap().unified_address;
        let claim = state
            .0
            .store
            .claim_address_faucet(&address, 123, "replay-address")
            .unwrap();
        state
            .0
            .store
            .record_address_prepared(&claim.id, "original-txid", b"original bytes", 140)
            .unwrap();
        state
            .0
            .store
            .confirm_address(&claim.id, "original-txid", "original-block")
            .unwrap();
        let body =
            json!({"address":address,"amount_zatoshi":123,"idempotency_key":"replay-address"});

        for _ in 0..2 {
            let response = router(state.clone())
                .oneshot(
                    Request::post("/api/v1/faucet/address")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(result["txid"], "original-txid");
            assert_eq!(result["block_hash"], "original-block");
            assert_eq!(result["status"], "confirmed");
        }
        let mut conflict = body;
        conflict["amount_zatoshi"] = json!(124);
        let response = router(state)
            .oneshot(
                Request::post("/api/v1/faucet/address")
                    .header("content-type", "application/json")
                    .body(Body::from(conflict.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn explorer_rejects_malformed_addresses_before_reaching_zakura() {
        let (state, _dir) = state_with_local_wallet();
        let account = state.0.store.account(1).unwrap();
        assert!(require_transparent_address(&account.transparent_address).is_ok());

        let response = router(state)
            .oneshot(
                Request::get("/api/v1/addresses/tmOOOOOOOOOOOOOOOOOOOOOOOO")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn explorer_reports_unknown_blocks_and_transactions_as_404() {
        // A node that knows nothing, answering with Zakura's not-found codes.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node = format!("http://{}", listener.local_addr().unwrap());
        let not_found = Router::new().route(
            "/",
            post(|Json(req): Json<Value>| async move {
                let code = if req["method"] == "getblock" { -8 } else { -5 };
                Json(json!({"error": {"code": code, "message": "not found"}}))
            }),
        );
        tokio::spawn(async move { axum::serve(listener, not_found).await });

        let (state, _dir) = state_with_local_wallet();
        let state = AppState::new(
            state.0.store.clone(),
            state.0.wallet.clone(),
            node,
            "test".into(),
        );
        let hash = "a".repeat(64);
        for path in [
            "/api/v1/blocks/999999".to_owned(),
            format!("/api/v1/transactions/{hash}"),
            "/api/v1/search?q=999999".to_owned(),
            format!("/api/v1/search?q={hash}"),
        ] {
            let response = router(state.clone())
                .oneshot(Request::get(&path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn search_does_not_mask_a_block_node_failure_as_404() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node = format!("http://{}", listener.local_addr().unwrap());
        let failing = Router::new().route(
            "/",
            post(|Json(req): Json<Value>| async move {
                let code = if req["method"] == "getblock" { -28 } else { -5 };
                Json(json!({"error": {"code": code, "message": "injected"}}))
            }),
        );
        tokio::spawn(async move { axum::serve(listener, failing).await });

        let (state, _dir) = state_with_local_wallet();
        let state = AppState::new(
            state.0.store.clone(),
            state.0.wallet.clone(),
            node,
            "t".into(),
        );
        let uri = format!("/api/v1/search?q={}", "a".repeat(64));
        let response = router(state)
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_error_logs_full_rpc_error_chain() {
        let error: RpcError =
            serde_json::from_value(json!({"code": -28, "message": "warming up"})).unwrap();
        let error = anyhow::Error::new(error)
            .context("Zakura getblock failed")
            .context("loading block details");
        let (response, logs) = capture_logs(|| ApiError::from(error).into_response());

        assert_eq!(logs.lines().count(), 1, "{logs}");
        assert!(logs.contains("ERROR"), "{logs}");
        assert!(
            logs.contains(
                "error=loading block details: Zakura getblock failed: RPC error -28: warming up"
            ),
            "{logs}"
        );
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({"error": {"message": "loading block details", "status": 500}})
        );
    }

    #[tokio::test]
    async fn api_error_logs_skip_expected_failures() {
        for (error, status) in [
            (
                anyhow::Error::new(IdempotencyConflict),
                StatusCode::CONFLICT,
            ),
            (
                anyhow::Error::new(PaymentError::InsufficientFunds {
                    available: 100_000_000,
                    required: 100_010_000,
                }),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                anyhow::Error::new(PaymentError::TransparentMemo),
                StatusCode::BAD_REQUEST,
            ),
            (
                anyhow::Error::new(PaymentError::TreasuryExhausted),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let error = error.context("payment could not complete");
            let (response, logs) = capture_logs(|| ApiError::from(error).into_response());
            assert!(logs.is_empty(), "{status}: {logs}");
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                json!({"error": {"message": "payment could not complete", "status": status.as_u16()}})
            );
        }
    }

    #[tokio::test]
    async fn api_error_logs_skip_explorer_misses() {
        for code in [-5, -8] {
            let error: RpcError =
                serde_json::from_value(json!({"code": code, "message": "not found"})).unwrap();
            let error = anyhow::Error::new(error).context("Zakura getblock failed");
            let (response, logs) = capture_logs(|| not_found(error, NO_BLOCK).into_response());
            assert!(logs.is_empty(), "{code}: {logs}");
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                json!({"error": {"message": NO_BLOCK, "status": 404}})
            );
        }
    }

    #[test]
    fn a_dry_treasury_is_reported_as_unavailable() {
        assert_eq!(
            ApiError::from(anyhow::Error::new(PaymentError::TreasuryExhausted)).status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn an_unaffordable_send_is_reported_as_a_client_error() {
        assert_eq!(
            ApiError::from(anyhow::Error::new(PaymentError::InsufficientFunds {
                available: 100_000_000,
                required: 100_010_000,
            }))
            .status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    /// An unknown pool is a client error, not the wallet's internal `bail!`
    /// surfacing as a 500 that the dashboard reports as an unexpected error.
    #[tokio::test]
    async fn send_quote_rejects_unknown_pools_as_bad_requests() {
        let (state, _dir) = state_with_local_wallet();
        let app = router(state);
        for (body, message) in [
            (
                json!({"from_account": 1, "source_pool": "sapling", "destination_pool": "ironwood"}),
                "source_pool must be transparent or ironwood",
            ),
            (
                json!({"from_account": 1, "source_pool": "ironwood", "destination_pool": "sapling"}),
                "destination_pool must be transparent or ironwood",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/api/v1/send/quote")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(
                String::from_utf8_lossy(&bytes).contains(message),
                "expected {message} in {bytes:?}"
            );
        }
    }

    struct RecordingFaucetRuntime {
        events: Mutex<Vec<String>>,
        funds_available: AtomicBool,
        reward_discovered: AtomicBool,
        height: AtomicUsize,
        cursor: Mutex<Option<crate::db::TreasuryCursor>>,
    }

    impl Default for RecordingFaucetRuntime {
        fn default() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                funds_available: AtomicBool::new(false),
                reward_discovered: AtomicBool::new(false),
                height: AtomicUsize::new(2),
                cursor: Mutex::new(None),
            }
        }
    }

    #[async_trait::async_trait]
    impl FaucetRuntime for RecordingFaucetRuntime {
        async fn prepare_payment<F>(
            &self,
            _activity_id: Option<&str>,
            _seed: &str,
            _destination: &str,
            _amount_zatoshi: u64,
            record: F,
        ) -> anyhow::Result<PreparedPayment>
        where
            F: FnOnce(PreparedPayment) -> anyhow::Result<PreparedPayment> + Send + 'static,
        {
            self.events.lock().unwrap().push("prepare".into());
            if self.funds_available.load(Ordering::SeqCst) {
                record(PreparedPayment {
                    txid: "recovered-txid".into(),
                    raw_transaction: b"recovered transaction".to_vec(),
                    expiry_height: 240,
                })
            } else {
                Err(anyhow::Error::new(PaymentError::InsufficientFunds {
                    available: 0,
                    required: 100_010_000,
                }))
            }
        }
        async fn chain_height(&self) -> anyhow::Result<u64> {
            Ok(self.height.load(Ordering::SeqCst) as u64)
        }
        async fn block_hash(&self, height: u32) -> anyhow::Result<String> {
            Ok(format!("{height:064x}"))
        }
        async fn synchronize_latest(&self) -> anyhow::Result<()> {
            self.events.lock().unwrap().push("sync".into());
            Ok(())
        }
        async fn treasury_cursor(&self) -> anyhow::Result<Option<crate::db::TreasuryCursor>> {
            Ok(self.cursor.lock().unwrap().clone())
        }
        async fn initialize_treasury_cursor(
            &self,
            receiver: &str,
            height: u32,
            hash: &str,
        ) -> anyhow::Result<crate::db::TreasuryCursor> {
            let cursor = crate::db::TreasuryCursor {
                receiver: receiver.into(),
                height,
                block_hash: hash.into(),
            };
            *self.cursor.lock().unwrap() = Some(cursor.clone());
            Ok(cursor)
        }
        async fn discover_reward(
            &self,
            cursor: &crate::db::TreasuryCursor,
            _treasury: &Account,
        ) -> anyhow::Result<crate::db::TreasuryCursor> {
            assert_eq!(cursor.height, 1);
            self.events.lock().unwrap().push("discover:2".into());
            self.reward_discovered.store(true, Ordering::SeqCst);
            let cursor = crate::db::TreasuryCursor {
                receiver: cursor.receiver.clone(),
                height: 2,
                block_hash: format!("{:064x}", 2),
            };
            *self.cursor.lock().unwrap() = Some(cursor.clone());
            Ok(cursor)
        }
        async fn shield_coinbase(
            &self,
            _seed: &str,
            _treasury: &Account,
            minimum_net: u64,
        ) -> anyhow::Result<()> {
            assert_eq!(minimum_net, 100_010_000);
            self.events.lock().unwrap().push("shield".into());
            if !self.reward_discovered.load(Ordering::SeqCst) {
                return Err(anyhow::Error::new(PaymentError::TreasuryExhausted));
            }
            self.funds_available.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn mine_and_sync(&self, blocks: u32) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(format!("mine:{blocks}"));
            self.height.fetch_add(blocks as usize, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn insufficient_funds_discovers_only_the_needed_mature_reward_and_retries() {
        let runtime = RecordingFaucetRuntime::default();
        let treasury = Account {
            id: TREASURY_ACCOUNT_ID,
            name: "Account 6".into(),
            unified_address: "uregtest-treasury".into(),
            transparent_address: "tmTreasury".into(),
            unified_full_viewing_key: None,
            transparent_zatoshi: 0,
            ironwood_zatoshi: 0,
        };

        let prepared = prepare_with_replenishment(
            &runtime,
            None,
            "seed",
            &treasury,
            "uregtest-recipient",
            100_000_000,
            Ok,
        )
        .await
        .unwrap();
        assert_eq!(prepared.txid, "recovered-txid");
        assert_eq!(runtime.cursor.lock().unwrap().as_ref().unwrap().height, 2);
        assert_eq!(
            runtime.events.into_inner().unwrap(),
            [
                "prepare",
                "shield",
                "mine:99",
                "discover:2",
                "shield",
                "mine:1",
                "prepare"
            ]
        );
    }

    #[derive(Default)]
    struct RecordingPaymentSubmitter {
        broadcasts: Mutex<Vec<Vec<u8>>>,
        lookups: Mutex<VecDeque<Result<bool, &'static str>>>,
        recovered: Mutex<Option<PreparedPayment>>,
        height: AtomicU64,
        fail_next: AtomicBool,
        queued_next: AtomicBool,
    }

    #[tokio::test]
    async fn address_faucet_reconciles_the_original_txid_after_a_lost_broadcast_response() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let payment = store
            .claim_address_faucet("external-regtest-address", 12_000, "same-address-intent")
            .unwrap();
        store
            .record_address_prepared(&payment.id, "original-txid", b"original signed bytes", 140)
            .unwrap();
        drop(store);

        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let payment = store
            .claim_address_faucet("external-regtest-address", 12_000, "same-address-intent")
            .unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([
                Ok(false),
                Err("node unavailable"),
                Ok(true),
            ])),
            height: AtomicU64::new(139),
            fail_next: AtomicBool::new(true),
            ..Default::default()
        };

        assert!(
            submit_address_prepared(&store, &runtime, &payment)
                .await
                .is_err()
        );
        let same = store
            .claim_address_faucet("external-regtest-address", 12_000, "same-address-intent")
            .unwrap();
        let resumed = submit_address_prepared(&store, &runtime, &same)
            .await
            .unwrap();
        let AddressSubmission::Broadcast(resumed) = resumed else {
            panic!("original payment must resume")
        };
        assert_eq!(resumed.txid, "original-txid");
        assert_eq!(resumed.status, "broadcast");
        assert_eq!(
            runtime.broadcasts.into_inner().unwrap(),
            [b"original signed bytes"]
        );
    }

    #[async_trait::async_trait]
    impl PaymentSubmitter for RecordingPaymentSubmitter {
        async fn transaction_known(&self, _txid: &str) -> anyhow::Result<bool> {
            self.lookups
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(false))
                .map_err(anyhow::Error::msg)
        }

        async fn chain_height(&self) -> anyhow::Result<u64> {
            Ok(self.height.load(Ordering::SeqCst))
        }

        async fn broadcast(&self, raw_transaction: &[u8]) -> anyhow::Result<()> {
            self.broadcasts
                .lock()
                .unwrap()
                .push(raw_transaction.to_vec());
            if self.fail_next.swap(false, Ordering::SeqCst) {
                anyhow::bail!("response lost");
            }
            if self.queued_next.swap(false, Ordering::SeqCst) {
                crate::wallet::check_broadcast_response(
                    zcash_client_backend::proto::service::SendResponse {
                        error_code: -1,
                        error_message:
                            "transaction dropped because it is already queued for download".into(),
                    },
                )?;
            }
            Ok(())
        }

        async fn recover_prepared(&self, _txid: &str) -> anyhow::Result<Option<PreparedPayment>> {
            Ok(self.recovered.lock().unwrap().take())
        }
    }

    #[tokio::test]
    async fn retry_submits_the_same_prepared_transaction_after_a_lost_response() {
        for (to, source, destination) in [
            (2, "ironwood", "ironwood"),
            (1, "ironwood", "transparent"),
            (1, "transparent", "ironwood"),
        ] {
            let store = Store::open(":memory:").unwrap();
            store.initialize().unwrap();
            let claim = store
                .claim_transfer(
                    1,
                    Recipient::Account(to),
                    source,
                    destination,
                    12_000,
                    "same",
                    None,
                )
                .unwrap();
            store
                .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
                .unwrap();
            let prepared = store.activity_for_key("same").unwrap().unwrap();
            let runtime = RecordingPaymentSubmitter {
                lookups: Mutex::new(VecDeque::from([
                    Ok(false),
                    Err("node unavailable"),
                    Ok(false),
                ])),
                height: AtomicU64::new(139),
                fail_next: AtomicBool::new(true),
                ..Default::default()
            };

            assert!(submit_prepared(&store, &runtime, &prepared).await.is_err());
            assert_eq!(
                store.activity_for_key("same").unwrap().unwrap().status,
                "prepared"
            );

            let broadcast = submit_prepared(&store, &runtime, &prepared).await.unwrap();
            assert!(matches!(broadcast, PreparedSubmission::Broadcast(_)));
            assert_eq!(
                runtime.broadcasts.into_inner().unwrap(),
                [b"signed transaction", b"signed transaction"]
            );
        }
    }

    #[tokio::test]
    async fn a_forgotten_broadcast_is_resubmitted_from_the_saved_bytes() {
        let (store, prepared) = prepared_payment(140);
        let broadcast = store.mark_broadcast(&prepared.id, &prepared.txid).unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false)])),
            height: AtomicU64::new(139),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &broadcast).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Broadcast(_)));
        assert_eq!(
            runtime.broadcasts.into_inner().unwrap(),
            [b"signed transaction"]
        );
        assert_eq!(
            store.activity_for_key("same").unwrap().unwrap().status,
            "broadcast"
        );
    }

    #[tokio::test]
    async fn a_lookup_failure_does_not_broadcast_or_change_status() {
        let (store, prepared) = prepared_payment(140);
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Err("node unavailable")])),
            height: AtomicU64::new(139),
            ..Default::default()
        };

        assert!(submit_prepared(&store, &runtime, &prepared).await.is_err());
        assert!(runtime.broadcasts.into_inner().unwrap().is_empty());
        assert_eq!(
            store.activity_for_key("same").unwrap().unwrap().status,
            "prepared"
        );
    }

    #[tokio::test]
    async fn queued_retry_keeps_the_original_payment_pending_until_confirmation() {
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
                "same-queued",
                None,
            )
            .unwrap();
        let prepared = store
            .record_prepared(&claim.id, "original-txid", b"original signed bytes", 140)
            .unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        store.initialize().unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false), Ok(true)])),
            height: AtomicU64::new(139),
            queued_next: AtomicBool::new(true),
            ..Default::default()
        };
        let PreparedSubmission::Broadcast(pending) =
            submit_prepared(&store, &runtime, &prepared).await.unwrap()
        else {
            panic!("queued bytes must not be replaced");
        };
        assert_eq!(pending.id, prepared.id);
        assert_eq!(pending.txid, "original-txid");
        assert_eq!(pending.status, "broadcast");
        assert!(pending.block_hash.is_none());
        assert_eq!(
            store
                .prepared_transaction(&pending.id)
                .unwrap()
                .raw_transaction,
            b"original signed bytes"
        );
        // A later lookup observes the same transaction without another broadcast.
        assert!(matches!(
            submit_prepared(&store, &runtime, &pending).await.unwrap(),
            PreparedSubmission::Broadcast(_)
        ));
        assert_eq!(
            *runtime.broadcasts.lock().unwrap(),
            [b"original signed bytes"]
        );
        let same = store
            .claim_transfer(
                1,
                Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "same-queued",
                None,
            )
            .unwrap();
        assert_eq!(same.id, pending.id);
        assert_eq!(same.txid, pending.txid);
    }

    #[tokio::test]
    async fn queued_external_faucet_retry_preserves_its_payment_and_bytes() {
        let store = Store::open(":memory:").unwrap();
        store.initialize().unwrap();
        let claim = store
            .claim_address_faucet("external-regtest-address", 12_000, "queued-address")
            .unwrap();
        let prepared = store
            .record_address_prepared(&claim.id, "address-txid", b"address signed bytes", 140)
            .unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false)])),
            height: AtomicU64::new(139),
            queued_next: AtomicBool::new(true),
            ..Default::default()
        };
        let AddressSubmission::Broadcast(pending) =
            submit_address_prepared(&store, &runtime, &prepared)
                .await
                .unwrap()
        else {
            panic!("queued payment must remain pending");
        };
        assert_eq!(pending.id, prepared.id);
        assert_eq!(pending.txid, prepared.txid);
        assert_eq!(pending.status, "broadcast");
        assert!(pending.block_hash.is_none());
        assert_eq!(
            *runtime.broadcasts.lock().unwrap(),
            [b"address signed bytes"]
        );
    }

    #[tokio::test]
    async fn a_broadcast_error_is_success_when_the_transaction_is_known() {
        let (store, prepared) = prepared_payment(140);
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false), Ok(true)])),
            height: AtomicU64::new(139),
            fail_next: AtomicBool::new(true),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &prepared).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Broadcast(_)));
        assert_eq!(
            store.activity_for_key("same").unwrap().unwrap().status,
            "broadcast"
        );
    }

    #[tokio::test]
    async fn a_known_transaction_does_not_require_saved_bytes() {
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
        let prepared = store
            .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
            .unwrap();
        rusqlite::Connection::open(path)
            .unwrap()
            .execute("DELETE FROM prepared_payments", [])
            .unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(true)])),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &prepared).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Broadcast(_)));
        assert_eq!(
            store.activity_for_key("same").unwrap().unwrap().status,
            "broadcast"
        );
    }

    #[tokio::test]
    async fn a_legacy_broadcast_recovers_bytes_from_the_wallet() {
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
        let prepared = store
            .record_prepared(&claim.id, "real-txid", b"signed transaction", 140)
            .unwrap();
        let broadcast = store.mark_broadcast(&prepared.id, &prepared.txid).unwrap();
        rusqlite::Connection::open(path)
            .unwrap()
            .execute("DELETE FROM prepared_payments", [])
            .unwrap();
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false)])),
            recovered: Mutex::new(Some(PreparedPayment {
                txid: broadcast.txid.clone(),
                raw_transaction: b"wallet transaction".to_vec(),
                expiry_height: 140,
            })),
            height: AtomicU64::new(139),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &broadcast).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Broadcast(_)));
        assert_eq!(
            runtime.broadcasts.into_inner().unwrap(),
            [b"wallet transaction"]
        );
        assert_eq!(
            store
                .prepared_transaction(&broadcast.id)
                .unwrap()
                .raw_transaction,
            b"wallet transaction"
        );
    }

    #[tokio::test]
    async fn an_expired_missing_transaction_is_reset_for_preparation() {
        let (store, prepared) = prepared_payment(140);
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false)])),
            height: AtomicU64::new(140),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &prepared).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Expired(_)));
        assert!(runtime.broadcasts.into_inner().unwrap().is_empty());
        let retry = store.activity_for_key("same").unwrap().unwrap();
        assert_eq!(retry.status, "preparing");
        assert!(retry.txid.is_empty());
    }

    #[tokio::test]
    async fn a_transaction_seen_at_expiry_is_not_replaced() {
        let (store, prepared) = prepared_payment(140);
        let runtime = RecordingPaymentSubmitter {
            lookups: Mutex::new(VecDeque::from([Ok(false), Ok(true)])),
            height: AtomicU64::new(140),
            ..Default::default()
        };

        let result = submit_prepared(&store, &runtime, &prepared).await.unwrap();

        assert!(matches!(result, PreparedSubmission::Broadcast(_)));
        let current = store.activity_for_key("same").unwrap().unwrap();
        assert_eq!(current.txid, "real-txid");
        assert_eq!(current.status, "broadcast");
    }

    fn prepared_payment(expiry_height: u64) -> (Store, Activity) {
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
            .record_prepared(&claim.id, "real-txid", b"signed transaction", expiry_height)
            .unwrap();
        let prepared = store.activity_for_key("same").unwrap().unwrap();
        (store, prepared)
    }

    async fn api_request(
        state: &AppState,
        method: &str,
        uri: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(if method == "GET" {
                        Body::empty()
                    } else {
                        Body::from(body.to_string())
                    })
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn api_get(state: &AppState, uri: &str) -> (StatusCode, Value) {
        api_request(state, "GET", uri, Value::Null).await
    }

    async fn counting_rpc(chain_info: bool) -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_calls = calls.clone();
        let rpc = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let calls = rpc_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Json(if chain_info && request["method"] == "getblockchaininfo" {
                    json!({"result": {"chain": "regtest", "blocks": 101, "bestblockhash": "a".repeat(64)}, "error": null, "id": request["id"]})
                } else {
                    json!({"result": null, "error": {"code": -1, "message": "unavailable"}, "id": request["id"]})
                })
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
        (url, calls)
    }

    #[tokio::test]
    async fn activity_lookup_by_key_returns_every_state_regardless_of_list_limits() {
        let (state, _dir) = state_with_local_wallet();
        let store = &state.0.store;
        let preparing = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                1_000,
                "lookup-preparing",
                Some(""),
            )
            .unwrap();
        let pending = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "transparent",
                2_000,
                "lookup-pending",
                None,
            )
            .unwrap();
        store
            .record_prepared(&pending.id, "pending-txid", b"raw", 140)
            .unwrap();
        store.mark_broadcast(&pending.id, "pending-txid").unwrap();
        let failed = store
            .claim_transfer(
                2,
                crate::db::Recipient::Account(1),
                "ironwood",
                "ironwood",
                3_000,
                "lookup-failed",
                None,
            )
            .unwrap();
        store.fail(&failed.id, "insufficient funds").unwrap();
        let confirmed = store
            .claim_faucet(3, "ironwood", 4_000, "lookup-confirmed")
            .unwrap();
        store
            .record_prepared(&confirmed.id, "confirmed-txid", b"raw", 140)
            .unwrap();
        store
            .confirm(&confirmed.id, "confirmed-txid", "block")
            .unwrap();
        store
            .claim_address_faucet("tm-external", 5, "lookup-external")
            .unwrap();

        let (status, listed) = api_get(&state, "/api/v1/activity?limit=10").await;
        assert_eq!(status, StatusCode::OK);
        let listed: Vec<_> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row["id"].clone(),
                    row["status"].clone(),
                    row["txid"].clone(),
                )
            })
            .collect();
        assert_eq!(
            listed,
            [
                (
                    json!(confirmed.id),
                    json!("confirmed"),
                    json!("confirmed-txid")
                ),
                (json!(failed.id), json!("failed"), json!("")),
                (json!(pending.id), json!("broadcast"), json!("pending-txid")),
                (json!(preparing.id), json!("preparing"), json!("")),
            ]
        );

        for index in 0..101 {
            store
                .claim_transfer(
                    1,
                    crate::db::Recipient::Account(3),
                    "ironwood",
                    "ironwood",
                    1,
                    &format!("filler-{index:03}"),
                    None,
                )
                .unwrap();
        }
        let (_, listed) = api_get(&state, "/api/v1/activity?limit=100").await;
        assert_eq!(listed.as_array().unwrap().len(), 100);
        assert!(
            listed
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["id"] != json!(preparing.id))
        );

        for (key, id, status, txid, last_error) in [
            (
                "lookup-preparing",
                &preparing.id,
                "preparing",
                "",
                Value::Null,
            ),
            (
                "lookup-pending",
                &pending.id,
                "broadcast",
                "pending-txid",
                Value::Null,
            ),
            (
                "lookup-failed",
                &failed.id,
                "failed",
                "",
                json!("insufficient funds"),
            ),
            (
                "lookup-confirmed",
                &confirmed.id,
                "confirmed",
                "confirmed-txid",
                Value::Null,
            ),
        ] {
            let (code, found) = api_get(
                &state,
                &format!("/api/v1/activity?limit=1&idempotency_key={key}"),
            )
            .await;
            assert_eq!(code, StatusCode::OK, "{key}");
            let found = found.as_array().unwrap();
            assert_eq!(found.len(), 1, "{key}");
            assert_eq!(found[0]["id"], json!(id), "{key}");
            assert_eq!(found[0]["status"], status, "{key}");
            assert_eq!(found[0]["txid"], txid, "{key}");
            assert_eq!(found[0]["last_error"], last_error, "{key}");
        }
        for key in ["lookup-missing", "lookup-external"] {
            assert_eq!(
                api_get(&state, &format!("/api/v1/activity?idempotency_key={key}")).await,
                (StatusCode::OK, json!([]))
            );
        }
        let (code, _) = api_get(&state, "/api/v1/activity?idempotency_key=").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn address_faucet_status_reads_internal_and_external_records() {
        let (state, _dir) = state_with_local_wallet();
        let store = &state.0.store;
        let external = store
            .claim_address_faucet("tm-external", 50, "status-external")
            .unwrap();
        store
            .record_address_error(&external.id, "preparing", "", "treasury is syncing")
            .unwrap();
        let transparent = store
            .claim_faucet(2, "transparent", 60, "status-transparent")
            .unwrap();
        store
            .record_prepared(&transparent.id, "transparent-txid", b"raw", 140)
            .unwrap();
        store
            .mark_broadcast(&transparent.id, "transparent-txid")
            .unwrap();
        let ironwood = store
            .claim_faucet(3, "ironwood", 70, "status-ironwood")
            .unwrap();
        store
            .record_prepared(&ironwood.id, "ironwood-txid", b"raw", 140)
            .unwrap();
        store
            .confirm(&ironwood.id, "ironwood-txid", "ironwood-block")
            .unwrap();
        let external_address = store
            .account(TREASURY_ACCOUNT_ID)
            .unwrap()
            .transparent_address;
        let confirmed_external = store
            .claim_address_faucet(&external_address, 80, "status-confirmed-external")
            .unwrap();
        store
            .record_address_prepared(&confirmed_external.id, "external-txid", b"raw", 140)
            .unwrap();
        store
            .confirm_address(&confirmed_external.id, "external-txid", "external-block")
            .unwrap();
        store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                90,
                "status-send",
                None,
            )
            .unwrap();

        let transparent_address = store.account(2).unwrap().transparent_address;
        let unified_address = store.account(3).unwrap().unified_address;
        for (key, expected) in [
            (
                "status-external",
                json!({"address": "tm-external", "amount_zatoshi": 50, "txid": "", "block_hash": null, "status": "preparing", "last_error": "treasury is syncing"}),
            ),
            (
                "status-transparent",
                json!({"address": transparent_address, "amount_zatoshi": 60, "txid": "transparent-txid", "block_hash": null, "status": "broadcast", "last_error": null}),
            ),
            (
                "status-ironwood",
                json!({"address": unified_address, "amount_zatoshi": 70, "txid": "ironwood-txid", "block_hash": "ironwood-block", "status": "confirmed", "last_error": null}),
            ),
            (
                "status-confirmed-external",
                json!({"address": external_address, "amount_zatoshi": 80, "txid": "external-txid", "block_hash": "external-block", "status": "confirmed", "last_error": null}),
            ),
        ] {
            assert_eq!(
                api_get(
                    &state,
                    &format!("/api/v1/faucet/address?idempotency_key={key}")
                )
                .await,
                (StatusCode::OK, expected),
                "{key}"
            );
        }

        for (key, address, amount) in [
            ("status-ironwood", unified_address.as_str(), 70),
            ("status-confirmed-external", external_address.as_str(), 80),
        ] {
            let (_, read) = api_get(
                &state,
                &format!("/api/v1/faucet/address?idempotency_key={key}"),
            )
            .await;
            let (code, replay) = api_request(
                &state,
                "POST",
                "/api/v1/faucet/address",
                json!({"address": address, "amount_zatoshi": amount, "idempotency_key": key}),
            )
            .await;
            assert_eq!((code, replay), (StatusCode::OK, read), "{key}");
        }

        for key in ["status-send", "status-missing"] {
            let (code, body) = api_get(
                &state,
                &format!("/api/v1/faucet/address?idempotency_key={key}"),
            )
            .await;
            assert_eq!(code, StatusCode::NOT_FOUND, "{key}");
            assert_eq!(body["error"]["status"], 404, "{key}");
        }
        assert_eq!(
            api_get(&state, "/api/v1/faucet/address?idempotency_key=")
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            api_get(&state, "/api/v1/faucet/address").await.0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn stored_status_reads_have_no_wallet_or_node_side_effects() {
        let (state, dir) = state_with_local_wallet();
        let (rpc_url, calls) = counting_rpc(true).await;
        let state = AppState::new(
            state.0.store.clone(),
            state.0.wallet.clone(),
            rpc_url,
            "test".into(),
        );
        let store = &state.0.store;
        let send = store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                1_000,
                "read-send",
                None,
            )
            .unwrap();
        store
            .claim_faucet(2, "transparent", 2_000, "read-internal")
            .unwrap();
        let external = store
            .claim_address_faucet("tm-external", 3_000, "read-external")
            .unwrap();
        let activities = store.unresolved_activities().unwrap();
        let faucets = store.unresolved_address_faucets().unwrap();

        for uri in [
            "/api/v1/activity",
            "/api/v1/activity?idempotency_key=read-send",
            "/api/v1/faucet/address?idempotency_key=read-internal",
            "/api/v1/faucet/address?idempotency_key=read-external",
            "/api/v1/faucet/address?idempotency_key=read-missing",
        ] {
            api_get(&state, uri).await;
        }

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.unresolved_activities().unwrap(), activities);
        assert_eq!(store.unresolved_address_faucets().unwrap(), faucets);
        for id in [&send.id, &external.id] {
            assert!(store.prepared_transaction(id).is_err());
            assert!(state.0.wallet.prepared(id).await.unwrap().is_none());
        }
        drop(dir);
    }

    #[tokio::test]
    async fn health_and_chain_effects_wait_until_startup_finishes() {
        let (state, _dir) = state_with_local_wallet();
        let (rpc_url, calls) = counting_rpc(true).await;
        let state = AppState::new(
            state.0.store.clone(),
            state.0.wallet.clone(),
            rpc_url,
            "test".into(),
        );
        state.0.wallet_snapshot.write().await.status.last_success_at = Some(1);
        let account = state.0.store.account(2).unwrap();

        let (code, health) = api_get(&state, "/api/v1/health").await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(health["ok"], false);
        assert_eq!(health["node"]["blocks"], 101);
        let health_calls = calls.load(Ordering::SeqCst);

        for (uri, body) in [
            ("/api/v1/send", valid_send()),
            (
                "/api/v1/faucet",
                json!({"account_id": 2, "pool": "ironwood", "amount_zatoshi": 1_000, "idempotency_key": "startup-faucet"}),
            ),
            (
                "/api/v1/faucet/address",
                json!({"address": account.transparent_address, "amount_zatoshi": 1_000, "idempotency_key": "startup-address"}),
            ),
            ("/api/v1/mine", json!({"blocks": 1})),
            (
                "/api/v1/mining/jobs",
                json!({"blocks": 1, "idempotency_key": "startup-mining"}),
            ),
        ] {
            let (code, body) = api_request(&state, "POST", uri, body).await;
            assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
            assert_eq!(
                body["error"]["message"],
                "the environment is still starting"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), health_calls);
        assert!(state.0.store.activities(100).unwrap().is_empty());
        assert!(
            state
                .0
                .store
                .unresolved_address_faucets()
                .unwrap()
                .is_empty()
        );
        assert!(state.0.mining.latest().await.is_none());

        state.mark_started();
        let (code, health) = api_get(&state, "/api/v1/health").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(health["ok"], true);
    }

    #[tokio::test]
    async fn server_store_repairs_preparation_from_the_wallet_journal() {
        let (mut state, dir) = state_with_local_wallet();
        let claim = state
            .0
            .store
            .claim_transfer(
                1,
                crate::db::Recipient::Account(2),
                "ironwood",
                "ironwood",
                12_000,
                "journal-repair",
                None,
            )
            .unwrap();
        rusqlite::Connection::open(dir.path().join("wallet.db"))
            .unwrap()
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![claim.id, "journal-txid", b"journaled signed bytes", 150_u64],
            )
            .unwrap();

        let rpc = Router::new().route(
            "/",
            post(|Json(request): Json<Value>| async move {
                let result = match request["method"].as_str().unwrap() {
                    "getblockchaininfo" => json!({"chain":"regtest","blocks":100}),
                    "getrawtransaction" => json!({
                        "txid":"journal-txid",
                        "confirmations":1,
                        "blockhash":"journal-block"
                    }),
                    method => panic!("unexpected RPC method {method}"),
                };
                Json(json!({
                    "jsonrpc":"2.0",
                    "id":request["id"],
                    "result":result,
                    "error":null
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
        Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);

        let intent = state
            .0
            .store
            .unresolved_activities()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        progress_activity(&state, &intent, claim.clone())
            .await
            .unwrap();
        let repaired = state
            .0
            .store
            .activity_for_key("journal-repair")
            .unwrap()
            .unwrap();

        assert_eq!(
            (repaired.status.as_str(), repaired.txid.as_str()),
            ("confirmed", "journal-txid")
        );
        assert_eq!(repaired.block_hash.as_deref(), Some("journal-block"));
        assert_eq!(
            state.0.store.prepared_transaction(&claim.id).unwrap(),
            PreparedTransaction {
                raw_transaction: b"journaled signed bytes".to_vec(),
                expiry_height: 150
            }
        );
    }
}
