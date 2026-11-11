//! Readiness of the reflector stores that controllers read from.

use std::{hash::Hash, time::Duration};

use snafu::{ResultExt, Snafu};
use stackable_operator::kube::runtime::reflector::{Lookup, Store, store::WriterDropped};
use tokio::time::error::Elapsed;

/// Bounds the wait for the initial list of a store, well below the termination grace period, so
/// reconciles waiting on it do not delay the shutdown.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "the initial list did not complete within {}s",
        READY_TIMEOUT.as_secs()
    ))]
    Timeout { source: Elapsed },

    #[snafu(display("the watch has ended"))]
    Closed { source: WriterDropped },
}

/// Reconciles start before the initial lists complete, and an empty store looks like a deleted
/// object.
pub async fn wait_until_ready<K>(store: &Store<K>) -> Result<(), Error>
where
    K: 'static + Clone + Lookup,
    K::DynamicType: Eq + Hash + Clone,
{
    tokio::time::timeout(READY_TIMEOUT, store.wait_until_ready())
        .await
        .context(TimeoutSnafu)?
        .context(ClosedSnafu)
}
