//! The subscriptions a service delivers to, and every way they change: registration, the reconciliation of declared
//! ones, updates, deletion, secret rotation, fault policies, claiming and recording deliveries, redelivery and dead
//! letters. The hub keeps the subscriptions in memory, as the outbox reads them on every commit, and writes each
//! change through the caller's connection, which must be the service's single writer.

use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

use super::{
    DeclaredSubscription, DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription,
    SubscriptionSource, WebhookChange, validate_declared,
};
use crate::clock::wall_now;
use crate::events::NewEvent;
use crate::ids::IdScheme;
use crate::outbox;

/// Why a subscription change was refused.
#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("invalid webhook subscription: {0}")]
    Invalid(String),
    #[error("unknown webhook subscription {0}")]
    Unknown(Uuid),
    #[error(
        "webhook subscription {name} ({id}) is declared in the configuration (webhooks.subscriptions): change it there \
         and restart; through the API only `paused` can be changed, until the next restart"
    )]
    ManagedByConfiguration { id: Uuid, name: String },
    #[error("storage failure: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// The subscriptions and the rules for changing them.
pub struct WebhookHub {
    subscriptions: Vec<Subscription>,
    /// Declared in configuration, reconciled at startup and after a reset.
    declared: Vec<DeclaredSubscription>,
    /// How long a declared subscription's previous secret stays valid after the configuration changes it.
    secret_grace: chrono::Duration,
    /// The event types a subscription may ask for.
    event_types: &'static [&'static str],
}

impl WebhookHub {
    /// Loads the stored subscriptions. `declared` must have valid, unique names (checked here).
    pub fn load(
        conn: &Connection,
        declared: Vec<DeclaredSubscription>,
        secret_grace: chrono::Duration,
        event_types: &'static [&'static str],
    ) -> Result<Self, WebhookError> {
        validate_declared(&declared).map_err(WebhookError::Invalid)?;
        Ok(Self {
            subscriptions: outbox::subscriptions(conn)?,
            declared,
            secret_grace,
            event_types,
        })
    }

    /// Every subscription, deleted ones included, in creation order.
    pub fn subscriptions(&self) -> &[Subscription] {
        &self.subscriptions
    }

    /// Whether any of `events` has a subscriber.
    pub fn delivers(&self, events: &[NewEvent]) -> bool {
        events
            .iter()
            .any(|e| self.subscriptions.iter().any(|s| s.wants(e.event_type)))
    }

    /// Forgets every subscription, for a reset that emptied the tables.
    pub fn clear(&mut self) {
        self.subscriptions.clear();
    }

    /// Checks a subscription request (http(s) URL, known event types, secret length) and normalizes its event types
    /// (sorted, without duplicates).
    pub fn validate(&self, mut request: NewSubscription) -> Result<NewSubscription, WebhookError> {
        let parsed = reqwest::Url::parse(&request.url)
            .map_err(|e| WebhookError::Invalid(format!("url: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(WebhookError::Invalid("url must be http or https".into()));
        }
        if request.event_types.is_empty() {
            return Err(WebhookError::Invalid("eventTypes must not be empty".into()));
        }
        if let Some(unknown) = request
            .event_types
            .iter()
            .find(|t| !self.event_types.contains(&t.as_str()))
        {
            return Err(WebhookError::Invalid(format!(
                "unknown event type {unknown}"
            )));
        }
        if let Some(secret) = &request.secret
            && !(16..=256).contains(&secret.chars().count())
        {
            return Err(WebhookError::Invalid(
                "secret must be 16 to 256 characters".into(),
            ));
        }
        request.event_types.sort();
        request.event_types.dedup();
        Ok(request)
    }

    /// Registers a webhook endpoint for the given event types, created at `now` (domain time). It receives events
    /// committed from now on; earlier ones can be redelivered. With a `secret` (16–256 characters) every delivery is
    /// signed (see [`super::signing`]); with `ordered`, deliveries go out one at a time in sequence order.
    pub fn register(
        &mut self,
        conn: &Connection,
        request: NewSubscription,
        ids: IdScheme,
        now: DateTime<Utc>,
    ) -> Result<Subscription, WebhookError> {
        let request = self.validate(request)?;
        self.create(
            conn,
            request,
            None,
            SubscriptionSource::Api,
            (true, false),
            ids,
            now,
        )
    }

    /// Stores a new, already validated subscription.
    #[allow(clippy::too_many_arguments)]
    fn create(
        &mut self,
        conn: &Connection,
        request: NewSubscription,
        name: Option<String>,
        source: SubscriptionSource,
        (enabled, paused): (bool, bool),
        ids: IdScheme,
        now: DateTime<Utc>,
    ) -> Result<Subscription, WebhookError> {
        let NewSubscription {
            url,
            event_types,
            secret,
            ordered,
        } = request;
        let subscription = Subscription {
            id: ids.subscription(self.subscriptions.len() as u64 + 1),
            url,
            event_types,
            enabled,
            created_at: now,
            secret,
            ordered,
            faults: None,
            name,
            source,
            paused,
            previous_secret: None,
            deleted: false,
        };
        outbox::insert_subscription(conn, &subscription)?;
        self.subscriptions.push(subscription.clone());
        Ok(subscription)
    }

    /// Brings the subscriptions declared in configuration into the database, by name: creates the missing ones,
    /// updates the ones whose url, event types, secret, ordering, enabled or paused flag changed, and disables
    /// configuration-owned ones no longer declared (keeping their history). Subscriptions created through the API
    /// are left alone. Returns whether a subscription may have deliveries to send now.
    pub fn reconcile(
        &mut self,
        conn: &Connection,
        ids: IdScheme,
        now: DateTime<Utc>,
    ) -> Result<bool, WebhookError> {
        let declared = self.declared.clone();
        let mut wake = false;
        for entry in &declared {
            let wanted = self
                .validate(entry.subscription.clone())
                .map_err(|e| match e {
                    WebhookError::Invalid(message) => WebhookError::Invalid(format!(
                        "webhooks.subscriptions {}: {message}",
                        entry.name
                    )),
                    other => other,
                })?;
            let existing = self
                .subscriptions
                .iter()
                .position(|s| s.name.as_deref() == Some(entry.name.as_str()));
            match existing {
                None => {
                    let created = self.create(
                        conn,
                        wanted,
                        Some(entry.name.clone()),
                        SubscriptionSource::Config,
                        (entry.enabled, entry.paused),
                        ids,
                        now,
                    )?;
                    tracing::info!(name = %entry.name, id = %created.id, url = %created.url, "webhook subscription created from configuration");
                    wake |= created.enabled && !created.paused;
                }
                Some(index) => {
                    let current = &self.subscriptions[index];
                    if current.source != SubscriptionSource::Config {
                        return Err(WebhookError::Invalid(format!(
                            "webhooks.subscriptions {}: the name belongs to a subscription created through the API",
                            entry.name
                        )));
                    }
                    let previous_secret = if wanted.secret == current.secret {
                        current.previous_secret.clone()
                    } else {
                        self.grace(current.secret.clone(), wanted.secret.is_some())
                    };
                    let updated = Subscription {
                        url: wanted.url,
                        event_types: wanted.event_types,
                        secret: wanted.secret,
                        ordered: wanted.ordered,
                        enabled: entry.enabled,
                        paused: entry.paused,
                        previous_secret,
                        ..current.clone()
                    };
                    if updated != *current {
                        outbox::update_subscription(conn, &updated)?;
                        tracing::info!(name = %entry.name, id = %updated.id, url = %updated.url, enabled = updated.enabled, paused = updated.paused, "webhook subscription updated from configuration");
                        wake |= updated.enabled && !updated.paused;
                        self.subscriptions[index] = updated;
                    }
                }
            }
        }
        for subscription in &mut self.subscriptions {
            let still_declared = declared
                .iter()
                .any(|d| subscription.name.as_deref() == Some(d.name.as_str()));
            if subscription.source == SubscriptionSource::Config
                && subscription.enabled
                && !still_declared
            {
                subscription.enabled = false;
                outbox::update_subscription(conn, subscription)?;
                tracing::warn!(name = ?subscription.name, id = %subscription.id, "webhook subscription no longer in the configuration; disabled, its history kept");
            }
        }
        Ok(wake)
    }

    /// The previous secret to keep signing with after a change away from `old`: for the secret grace, when there was
    /// an old secret and there still is a new one.
    fn grace(&self, old: Option<String>, signed: bool) -> Option<(String, DateTime<Utc>)> {
        old.filter(|_| signed && self.secret_grace > chrono::Duration::zero())
            .map(|old| (old, wall_now() + self.secret_grace))
    }

    /// The index of a subscription that exists and is not deleted.
    fn live(&self, id: Uuid) -> Result<usize, WebhookError> {
        self.subscriptions
            .iter()
            .position(|s| s.id == id && !s.deleted)
            .ok_or(WebhookError::Unknown(id))
    }

    /// Refuses to change a configuration-owned subscription through the API.
    fn api_owned(&self, index: usize) -> Result<(), WebhookError> {
        let subscription = &self.subscriptions[index];
        match subscription.source {
            SubscriptionSource::Api => Ok(()),
            SubscriptionSource::Config => Err(WebhookError::ManagedByConfiguration {
                id: subscription.id,
                name: subscription.name.clone().unwrap_or_default(),
            }),
        }
    }

    /// Changes a subscription's URL, event types, ordering, or enabled and paused flags; unset fields stay. Pending
    /// deliveries keep their place and go to the new URL. A subscription declared in configuration only takes
    /// `paused`, which lasts until the next restart reapplies the configuration. Returns the subscription, and
    /// whether it may have deliveries to send now.
    pub fn update(
        &mut self,
        conn: &Connection,
        id: Uuid,
        change: WebhookChange,
    ) -> Result<(Subscription, bool), WebhookError> {
        let index = self.live(id)?;
        let WebhookChange {
            url,
            event_types,
            ordered,
            enabled,
            paused,
        } = change;
        if url.is_some() || event_types.is_some() || ordered.is_some() || enabled.is_some() {
            self.api_owned(index)?;
        }
        let current = &self.subscriptions[index];
        let wanted = self.validate(NewSubscription {
            url: url.unwrap_or_else(|| current.url.clone()),
            event_types: event_types.unwrap_or_else(|| current.event_types.clone()),
            secret: current.secret.clone(),
            ordered: ordered.unwrap_or(current.ordered),
        })?;
        let current = &self.subscriptions[index];
        let updated = Subscription {
            url: wanted.url,
            event_types: wanted.event_types,
            ordered: wanted.ordered,
            enabled: enabled.unwrap_or(current.enabled),
            paused: paused.unwrap_or(current.paused),
            ..current.clone()
        };
        let mut wake = false;
        if updated != *current {
            outbox::update_subscription(conn, &updated)?;
            wake = updated.enabled && !updated.paused;
            self.subscriptions[index] = updated;
        }
        Ok((self.subscriptions[index].clone(), wake))
    }

    /// Deletes a subscription created through the API: it disappears from the API, gets no new deliveries and sends
    /// none of its pending ones. Its deliveries and attempts stay in the database.
    pub fn delete(&mut self, conn: &Connection, id: Uuid) -> Result<(), WebhookError> {
        let index = self.live(id)?;
        self.api_owned(index)?;
        let mut deleted = self.subscriptions[index].clone();
        deleted.deleted = true;
        outbox::update_subscription(conn, &deleted)?;
        self.subscriptions[index] = deleted;
        Ok(())
    }

    /// Replaces a subscription's signing secret (API-created subscriptions only). For `grace` the previous secret
    /// stays valid: deliveries carry signatures under both, so the consumer can switch without rejecting any.
    pub fn rotate_secret(
        &mut self,
        conn: &Connection,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, WebhookError> {
        let index = self.live(id)?;
        self.api_owned(index)?;
        let current = &self.subscriptions[index];
        let wanted = self.validate(NewSubscription {
            url: current.url.clone(),
            event_types: current.event_types.clone(),
            secret: Some(secret),
            ordered: current.ordered,
        })?;
        let current = &self.subscriptions[index];
        let previous_secret = current
            .secret
            .clone()
            .filter(|old| Some(old) != wanted.secret.as_ref() && grace > chrono::Duration::zero())
            .map(|old| (old, wall_now() + grace));
        let updated = Subscription {
            secret: wanted.secret,
            previous_secret,
            ..current.clone()
        };
        outbox::update_subscription(conn, &updated)?;
        self.subscriptions[index] = updated;
        Ok(self.subscriptions[index].clone())
    }

    /// Sets (or with `None` clears) a subscription's delivery fault policy. It applies to attempts from the next
    /// claim on. Out-of-order holds are refused for an ordered subscription, which by definition never reorders.
    pub fn set_faults(
        &mut self,
        conn: &Connection,
        id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, WebhookError> {
        let subscription = self
            .subscriptions
            .iter_mut()
            .find(|s| s.id == id && !s.deleted)
            .ok_or(WebhookError::Unknown(id))?;
        if let Some(policy) = &policy {
            policy.validate().map_err(WebhookError::Invalid)?;
            if subscription.ordered && policy.out_of_order.is_some() {
                return Err(WebhookError::Invalid(
                    "outOfOrder cannot apply to an ordered subscription".into(),
                ));
            }
        }
        outbox::set_fault_policy(conn, id, policy.as_ref())?;
        subscription.faults = policy;
        Ok(subscription.clone())
    }

    /// Claims up to `limit` due deliveries, leased for `lease`, stamped with the service's reset `epoch`.
    pub fn claim(
        &self,
        conn: &mut Connection,
        limit: u32,
        lease: Duration,
        epoch: u64,
    ) -> Result<Vec<DeliveryJob>, WebhookError> {
        let now = wall_now();
        let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        let mut jobs = outbox::claim_deliveries(conn, now, lease_until, limit)?;
        for job in &mut jobs {
            job.epoch = epoch;
        }
        Ok(jobs)
    }

    /// Records delivery results, except those claimed before the reset that started `epoch`: their deliveries are
    /// gone (or, with deterministic IDs, a new delivery may have the same ID).
    pub fn record(
        &self,
        conn: &mut Connection,
        results: &[DeliveryResult],
        epoch: u64,
    ) -> Result<(), WebhookError> {
        let now = wall_now();
        let current: Vec<DeliveryResult> = results
            .iter()
            .filter(|r| r.epoch == epoch)
            .cloned()
            .collect();
        if current.len() < results.len() {
            tracing::debug!(
                dropped = results.len() - current.len(),
                "dropped delivery results claimed before a reset"
            );
        }
        Ok(outbox::record_deliveries(conn, &current, now)?)
    }

    /// Queues the subscription's events after sequence `after` for delivery again (see
    /// [`outbox::redeliver`]); returns how many.
    pub fn redeliver(
        &self,
        conn: &mut Connection,
        id: Uuid,
        after: u64,
        ids: IdScheme,
    ) -> Result<usize, WebhookError> {
        let subscription = self
            .subscriptions
            .iter()
            .find(|s| s.id == id && !s.deleted)
            .ok_or(WebhookError::Unknown(id))?;
        Ok(outbox::redeliver(
            conn,
            subscription,
            after,
            wall_now(),
            &ids,
        )?)
    }

    /// Gives dead-lettered deliveries a fresh attempt budget: all, one subscription's, or the listed ones.
    pub fn retry_dead_letters(
        &self,
        conn: &mut Connection,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
    ) -> Result<usize, WebhookError> {
        if let Some(id) = subscription_id
            && !self.subscriptions.iter().any(|s| s.id == id && !s.deleted)
        {
            return Err(WebhookError::Unknown(id));
        }
        Ok(outbox::retry_dead_letters(
            conn,
            subscription_id,
            delivery_ids,
            wall_now(),
        )?)
    }
}
