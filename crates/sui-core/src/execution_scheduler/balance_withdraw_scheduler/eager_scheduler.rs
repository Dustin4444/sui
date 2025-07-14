// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;
use prometheus::{
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, IntCounterVec,
    IntGaugeVec, Registry,
};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    transaction::Reservation,
};
use tracing::{debug, trace};

use crate::execution_scheduler::balance_withdraw_scheduler::{
    balance_read::AccountBalanceRead,
    scheduler::{BalanceWithdrawSchedulerTrait, WithdrawReservations},
    BalanceSettlement, ScheduleResult, ScheduleStatus,
};

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct AccountState {
    /// The last known settled balance from the accumulator
    settled_balance: u64,
    /// Cumulative reservations since the last known settlement
    /// This tracks the sum of all reservations scheduled since the last settlement
    cumulative_reservations: u64,
    /// The accumulator version at which this balance was last settled
    last_settled_version: SequenceNumber,
    /// Whether an EntireBalance reservation has been made for this account
    /// If true, no further reservations can be scheduled until settlement
    entire_balance_reserved: bool,
}

impl AccountState {
    pub(super) fn new(balance: u64, version: SequenceNumber) -> Self {
        Self {
            settled_balance: balance,
            cumulative_reservations: 0,
            last_settled_version: version,
            entire_balance_reserved: false,
        }
    }

    /// Calculate the minimum guaranteed balance available for new reservations
    pub(super) fn minimum_guaranteed_balance(&self) -> u64 {
        if self.entire_balance_reserved {
            0
        } else {
            self.settled_balance
                .saturating_sub(self.cumulative_reservations)
        }
    }

    /// Try to reserve an amount from this account
    /// Returns true if the reservation was successful
    pub(super) fn try_reserve(&mut self, reservation: &Reservation) -> bool {
        match reservation {
            Reservation::MaxAmountU64(amount) => {
                if self.entire_balance_reserved {
                    return false;
                }
                let available = self.minimum_guaranteed_balance();
                if available >= *amount {
                    self.cumulative_reservations =
                        self.cumulative_reservations.saturating_add(*amount);
                    true
                } else {
                    false
                }
            }
            Reservation::EntireBalance => {
                if self.entire_balance_reserved || self.cumulative_reservations > 0 {
                    false
                } else if self.settled_balance > 0 {
                    self.entire_balance_reserved = true;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Apply a settlement to this account
    pub(super) fn apply_settlement(&mut self, new_balance: u64, version: SequenceNumber) {
        self.settled_balance = new_balance;
        self.cumulative_reservations = 0;
        self.entire_balance_reserved = false;
        self.last_settled_version = version;
    }
}

/// Tracks which consensus commit batches have been scheduled to prevent double scheduling
#[derive(Debug)]
struct ScheduledBatches {
    /// Maps accumulator version to whether that batch has been scheduled
    scheduled_versions: BTreeMap<SequenceNumber, bool>,
}

impl ScheduledBatches {
    fn new() -> Self {
        Self {
            scheduled_versions: BTreeMap::new(),
        }
    }

    /// Check if a batch has already been scheduled
    fn is_already_scheduled(&self, version: SequenceNumber) -> bool {
        self.scheduled_versions.contains_key(&version)
    }

    /// Mark a batch as scheduled
    fn mark_scheduled(&mut self, version: SequenceNumber) {
        self.scheduled_versions.insert(version, true);
    }

    /// Clean up old entries that are before the given version
    fn cleanup_before(&mut self, version: SequenceNumber) {
        self.scheduled_versions = self.scheduled_versions.split_off(&version);
    }
}

/// Metrics for tracking scheduler performance and behavior
pub struct EagerSchedulerMetrics {
    /// Count of scheduling outcomes by status
    pub schedule_outcome_counter: IntCounterVec,
    /// Number of accounts currently being tracked
    pub tracked_accounts_gauge: IntGaugeVec,
    /// Number of active reservations by type
    pub active_reservations_gauge: IntGaugeVec,
    /// Count of settlements processed
    pub settlements_processed_counter: IntCounterVec,
}

impl EagerSchedulerMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            schedule_outcome_counter: register_int_counter_vec_with_registry!(
                "eager_scheduler_schedule_outcome",
                "Count of scheduling outcomes by status",
                &["status"],
                registry,
            )
            .unwrap(),
            tracked_accounts_gauge: register_int_gauge_vec_with_registry!(
                "eager_scheduler_tracked_accounts",
                "Number of accounts currently being tracked",
                &["type"],
                registry,
            )
            .unwrap(),
            active_reservations_gauge: register_int_gauge_vec_with_registry!(
                "eager_scheduler_active_reservations",
                "Number of active reservations by type",
                &["type"],
                registry,
            )
            .unwrap(),
            settlements_processed_counter: register_int_counter_vec_with_registry!(
                "eager_scheduler_settlements_processed",
                "Count of settlements processed",
                &["type"],
                registry,
            )
            .unwrap(),
        }
    }
}

/// The eager balance withdrawal scheduler that optimistically schedules withdrawals
/// without waiting for settlements when sufficient balance can be guaranteed
pub(crate) struct EagerBalanceWithdrawScheduler {
    balance_read: Arc<dyn AccountBalanceRead>,
    /// Protected state that tracks account balances and reservations
    state: Arc<RwLock<EagerSchedulerState>>,
    /// Metrics for monitoring
    metrics: Option<EagerSchedulerMetrics>,
}

struct EagerSchedulerState {
    /// Track account states only for accounts with pending withdrawals
    account_states: BTreeMap<ObjectID, AccountState>,
    /// Track which consensus batches have been scheduled
    scheduled_batches: ScheduledBatches,
    /// The highest accumulator version we've processed
    highest_processed_version: SequenceNumber,
    /// The last settled accumulator version
    last_settled_version: SequenceNumber,
}

impl EagerBalanceWithdrawScheduler {
    pub fn new(
        balance_read: Arc<dyn AccountBalanceRead>,
        starting_accumulator_version: SequenceNumber,
    ) -> Arc<Self> {
        Arc::new(Self {
            balance_read,
            state: Arc::new(RwLock::new(EagerSchedulerState {
                account_states: BTreeMap::new(),
                scheduled_batches: ScheduledBatches::new(),
                highest_processed_version: starting_accumulator_version,
                last_settled_version: starting_accumulator_version,
            })),
            metrics: None,
        })
    }

    pub fn new_with_metrics(
        balance_read: Arc<dyn AccountBalanceRead>,
        starting_accumulator_version: SequenceNumber,
        registry: &Registry,
    ) -> Arc<Self> {
        Arc::new(Self {
            balance_read,
            state: Arc::new(RwLock::new(EagerSchedulerState {
                account_states: BTreeMap::new(),
                scheduled_batches: ScheduledBatches::new(),
                highest_processed_version: starting_accumulator_version,
                last_settled_version: starting_accumulator_version,
            })),
            metrics: Some(EagerSchedulerMetrics::new(registry)),
        })
    }

    /// Load balance for an account if not already tracked
    fn ensure_account_loaded(
        &self,
        state: &mut EagerSchedulerState,
        account_id: &ObjectID,
        accumulator_version: SequenceNumber,
    ) {
        if !state.account_states.contains_key(account_id) {
            let balance = self
                .balance_read
                .get_account_balance(account_id, accumulator_version);
            state
                .account_states
                .insert(*account_id, AccountState::new(balance, accumulator_version));
            trace!(
                "Loaded account {:?} with balance {} at version {:?}",
                account_id,
                balance,
                accumulator_version
            );
        }
    }

    /// Clean up accounts that no longer need tracking
    fn cleanup_accounts(&self, state: &mut EagerSchedulerState) {
        let _before_count = state.account_states.len();
        state.account_states.retain(|account_id, account_state| {
            let should_retain =
                account_state.cumulative_reservations > 0 || account_state.entire_balance_reserved;
            if !should_retain {
                trace!("Removing account {:?} from tracking", account_id);
            }
            should_retain
        });

        if let Some(metrics) = &self.metrics {
            metrics
                .tracked_accounts_gauge
                .with_label_values(&["total"])
                .set(state.account_states.len() as i64);

            let with_reservations = state
                .account_states
                .values()
                .filter(|s| s.cumulative_reservations > 0)
                .count();
            metrics
                .tracked_accounts_gauge
                .with_label_values(&["with_reservations"])
                .set(with_reservations as i64);

            let entire_balance_reserved = state
                .account_states
                .values()
                .filter(|s| s.entire_balance_reserved)
                .count();
            metrics
                .tracked_accounts_gauge
                .with_label_values(&["entire_balance_reserved"])
                .set(entire_balance_reserved as i64);
        }
    }
}

#[async_trait::async_trait]
impl BalanceWithdrawSchedulerTrait for EagerBalanceWithdrawScheduler {
    async fn schedule_withdraws(&self, withdraws: WithdrawReservations) {
        let mut state = self.state.write();

        // Check if this batch has already been scheduled
        if state
            .scheduled_batches
            .is_already_scheduled(withdraws.accumulator_version)
        {
            debug!(
                "Batch at version {:?} already scheduled",
                withdraws.accumulator_version
            );
            for (withdraw, sender) in withdraws.withdraws.into_iter().zip(withdraws.senders) {
                let _ = sender.send(ScheduleResult {
                    tx_digest: withdraw.tx_digest,
                    status: ScheduleStatus::AlreadyScheduled,
                });
                if let Some(metrics) = &self.metrics {
                    metrics
                        .schedule_outcome_counter
                        .with_label_values(&["already_scheduled"])
                        .inc();
                }
            }
            return;
        }

        // Mark this batch as scheduled
        state
            .scheduled_batches
            .mark_scheduled(withdraws.accumulator_version);
        state.highest_processed_version = state
            .highest_processed_version
            .max(withdraws.accumulator_version);

        // Process each transaction's withdrawals sequentially
        for (withdraw, sender) in withdraws.withdraws.into_iter().zip(withdraws.senders) {
            // First ensure all accounts in this transaction are loaded
            for account_id in withdraw.reservations.keys() {
                self.ensure_account_loaded(&mut state, account_id, withdraws.accumulator_version);
            }

            // Try to reserve all amounts atomically for this transaction
            let mut temp_states = Vec::new();
            let mut all_success = true;

            for (account_id, reservation) in &withdraw.reservations {
                let account_state = state.account_states.get_mut(account_id).unwrap();
                let original_state = account_state.clone();

                if account_state.try_reserve(reservation) {
                    temp_states.push((*account_id, original_state));
                } else {
                    all_success = false;
                    // Rollback any partial reservations
                    for (rollback_id, original) in temp_states {
                        *state.account_states.get_mut(&rollback_id).unwrap() = original;
                    }
                    break;
                }
            }

            let status = if all_success {
                debug!(
                    "Successfully scheduled withdraw {:?} with reservations {:?}",
                    withdraw.tx_digest, withdraw.reservations
                );
                ScheduleStatus::SufficientBalance
            } else {
                debug!(
                    "Insufficient balance for withdraw {:?} with reservations {:?}",
                    withdraw.tx_digest, withdraw.reservations
                );
                ScheduleStatus::InsufficientBalance
            };

            if let Some(metrics) = &self.metrics {
                let label = match status {
                    ScheduleStatus::SufficientBalance => "sufficient_balance",
                    ScheduleStatus::InsufficientBalance => "insufficient_balance",
                    ScheduleStatus::AlreadyScheduled => "already_scheduled",
                };
                metrics
                    .schedule_outcome_counter
                    .with_label_values(&[label])
                    .inc();
            }

            let _ = sender.send(ScheduleResult {
                tx_digest: withdraw.tx_digest,
                status,
            });
        }

        // Clean up accounts that no longer need tracking
        self.cleanup_accounts(&mut state);
    }

    async fn settle_balances(&self, settlement: BalanceSettlement) {
        let mut state = self.state.write();

        debug!(
            "Settling balances at version {:?} with {} changes",
            settlement.accumulator_version,
            settlement.balance_changes.len()
        );

        if let Some(metrics) = &self.metrics {
            metrics
                .settlements_processed_counter
                .with_label_values(&["total"])
                .inc();

            if !settlement.balance_changes.is_empty() {
                metrics
                    .settlements_processed_counter
                    .with_label_values(&["with_changes"])
                    .inc();
            }
        }

        // Update the last settled version
        state.last_settled_version = settlement.accumulator_version;

        // Apply balance changes to tracked accounts
        for (account_id, balance_change) in &settlement.balance_changes {
            if let Some(account_state) = state.account_states.get_mut(account_id) {
                // Calculate new balance from the change
                let new_balance = if *balance_change >= 0 {
                    account_state
                        .settled_balance
                        .saturating_add(*balance_change as u64)
                } else {
                    account_state
                        .settled_balance
                        .saturating_sub(balance_change.unsigned_abs() as u64)
                };

                account_state.apply_settlement(new_balance, settlement.accumulator_version);
                trace!(
                    "Applied settlement to account {:?}: change={}, new_balance={}",
                    account_id,
                    balance_change,
                    new_balance
                );
            }
        }

        // For any tracked accounts not in the settlement, we need to update their version
        // and fetch the latest balance
        let accounts_to_update: Vec<ObjectID> = state
            .account_states
            .iter()
            .filter(|(id, _)| !settlement.balance_changes.contains_key(id))
            .map(|(id, _)| *id)
            .collect();

        for account_id in accounts_to_update {
            let new_balance = self
                .balance_read
                .get_account_balance(&account_id, settlement.accumulator_version);
            if let Some(account_state) = state.account_states.get_mut(&account_id) {
                account_state.apply_settlement(new_balance, settlement.accumulator_version);
                trace!(
                    "Refreshed balance for account {:?}: new_balance={}",
                    account_id,
                    new_balance
                );
            }
        }

        // Clean up old scheduled batch entries
        state
            .scheduled_batches
            .cleanup_before(settlement.accumulator_version);

        // Clean up accounts that no longer need tracking
        self.cleanup_accounts(&mut state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_state_reservations() {
        let mut state = AccountState::new(100, SequenceNumber::from_u64(0));

        // Test MaxAmountU64 reservation
        assert!(state.try_reserve(&Reservation::MaxAmountU64(50)));
        assert_eq!(state.minimum_guaranteed_balance(), 50);
        assert!(state.try_reserve(&Reservation::MaxAmountU64(30)));
        assert_eq!(state.minimum_guaranteed_balance(), 20);
        assert!(!state.try_reserve(&Reservation::MaxAmountU64(30)));

        // Test EntireBalance reservation
        let mut state2 = AccountState::new(100, SequenceNumber::from_u64(0));
        assert!(state2.try_reserve(&Reservation::EntireBalance));
        assert_eq!(state2.minimum_guaranteed_balance(), 0);
        assert!(!state2.try_reserve(&Reservation::MaxAmountU64(1)));

        // Test EntireBalance after partial reservation
        let mut state3 = AccountState::new(100, SequenceNumber::from_u64(0));
        assert!(state3.try_reserve(&Reservation::MaxAmountU64(50)));
        assert!(!state3.try_reserve(&Reservation::EntireBalance));
    }

    #[test]
    fn test_settlement_resets_reservations() {
        let mut state = AccountState::new(100, SequenceNumber::from_u64(0));
        assert!(state.try_reserve(&Reservation::MaxAmountU64(80)));
        assert_eq!(state.minimum_guaranteed_balance(), 20);

        state.apply_settlement(150, SequenceNumber::from_u64(1));
        assert_eq!(state.minimum_guaranteed_balance(), 150);
        assert_eq!(state.cumulative_reservations, 0);
        assert!(!state.entire_balance_reserved);
    }
}
