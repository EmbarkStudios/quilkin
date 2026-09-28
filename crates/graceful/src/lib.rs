use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken as Token;
pub use tokio_util::task::TaskTracker;

pub mod health;

/// A root cancellation token, cancelling it will cancel all tokens cloned from it
pub struct RootToken(Token);

impl RootToken {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        Self(Token::new())
    }
}

/// Mainly for testing, creates a new root token
#[inline]
pub fn root() -> RootToken {
    RootToken::new()
}

/// A child cancellation token, cancelling it will not cancel the parent token
pub struct ChildToken(Token);

impl std::fmt::Debug for ChildToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

macro_rules! common {
    ($which:ident) => {
        impl $which {
            /// Creates a child token that can't cancel this token
            #[inline]
            pub fn child(&self) -> $crate::ChildToken {
                $crate::ChildToken(self.0.child_token())
            }

            /// <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html#method.drop_guard>
            #[inline]
            pub fn drop_guard(self) -> tokio_util::sync::DropGuard {
                self.0.drop_guard()
            }
        }

        impl Clone for $which {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        impl std::ops::Deref for $which {
            type Target = Token;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

common!(RootToken);
common!(ChildToken);

/// A child of the [`RootSpawner`] that tracks its own child tasks for graceful shutdown
#[derive(Clone)]
pub struct SubSpawner {
    tracker: TaskTracker,
    token: ChildToken,
}

impl From<ChildToken> for SubSpawner {
    fn from(token: ChildToken) -> Self {
        Self {
            tracker: TaskTracker::new(),
            token,
        }
    }
}

impl SubSpawner {
    #[inline]
    pub fn token(&self) -> ChildToken {
        self.token.clone()
    }

    /// Waits for all outstanding tasks to complete, or the specified timeout to expire
    #[inline]
    pub async fn wait(self, max: std::time::Duration) -> bool {
        self.tracker.close();
        tokio::time::timeout(max, self.tracker.wait())
            .await
            .is_ok_and(|_| true)
    }

    #[inline]
    pub async fn cancel_and_wait(self) {
        self.tracker.close();
        self.token.cancel();
        self.tracker.wait().await;
    }

    #[inline]
    pub fn into_tracker(self) -> TaskTracker {
        self.tracker
    }
}

impl std::ops::Deref for SubSpawner {
    type Target = TaskTracker;

    fn deref(&self) -> &Self::Target {
        &self.tracker
    }
}

/// A task spawner that awaits cancellation, or one or more of the registered tasks to complete
pub struct TaskSpawner {
    token: RootToken,
    set: tokio::task::JoinSet<eyre::Result<()>>,
    id_to_name: std::collections::BTreeMap<tokio::task::Id, (&'static str, AbortHandle)>,
}

impl TaskSpawner {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            token: RootToken::new(),
            set: tokio::task::JoinSet::new(),
            id_to_name: Default::default(),
        }
    }

    #[inline]
    pub fn push_async(
        &mut self,
        svc: &'static str,
        task: impl Future<Output = eyre::Result<()>> + Send + 'static,
    ) {
        let handle = self.set.spawn(task);
        if self.id_to_name.insert(handle.id(), (svc, handle)).is_some() {
            panic!("{svc} was already registered");
        }
    }

    #[inline]
    pub fn push_sync(
        &mut self,
        svc: &'static str,
        wait: impl FnOnce() -> eyre::Result<()> + Send + 'static,
    ) {
        let token = self.token.clone();
        let handle = self.set.spawn(async move {
            token.cancelled().await;
            tokio::task::block_in_place(wait)
        });

        if self.id_to_name.insert(handle.id(), (svc, handle)).is_some() {
            panic!("{svc} was already registered");
        }
    }

    #[inline]
    pub fn root(&self) -> RootToken {
        self.token.clone()
    }

    #[inline]
    pub fn child(&self) -> ChildToken {
        self.token.child()
    }

    #[inline]
    pub fn sub_spawner(&self) -> SubSpawner {
        self.token.child().into()
    }

    /// Waits for cancellation or one of the child tasks to finish, upon which it waits for the rest of the child tasks
    /// to finish
    pub async fn wait_cancellation_or_error(
        self,
        graceful_timeout: std::time::Duration,
        wait_timeout: std::time::Duration,
    ) -> Vec<(&'static str, eyre::Result<()>)> {
        let Self {
            token,
            mut set,
            mut id_to_name,
        } = self;

        let mut results = Vec::with_capacity(id_to_name.len());

        fn push(
            res: Result<(tokio::task::Id, eyre::Result<()>), tokio::task::JoinError>,
            results: &mut Vec<(&'static str, eyre::Result<()>)>,
            id_to_name: &mut std::collections::BTreeMap<
                tokio::task::Id,
                (&'static str, tokio::task::AbortHandle),
            >,
        ) {
            let (id, res) = match res {
                Ok((id, res)) => (id, res),
                Err(je) => {
                    let id = je.id();
                    let res = if je.is_panic() {
                        Err(eyre::eyre!("task paniced: {:?}", je.into_panic()))
                    } else {
                        Err(eyre::eyre!("task was cancelled"))
                    };

                    (id, res)
                }
            };

            let name = id_to_name
                .remove(&id)
                .map_or("<unknown task>", |(name, _)| name);
            results.push((name, res));
        }

        tokio::select! {
            _ = token.cancelled() => {
            }
            res = set.join_next_with_id() => {
                // Tasks spawned by this instance are supposed to live the lifetime of quilkin, so if one exits it's a
                // terminal condition, so let the rest of the tasks know they are cancelled so they can attempt to
                // gracefully shut down
                token.cancel();

                push(res.expect("no tasks are still running on the join set"), &mut results, &mut id_to_name);
            }
        };

        // Attempt to wait for all of the tasks to gracefully shut down
        if tokio::time::timeout(graceful_timeout, async {
            while let Some(res) = set.join_next_with_id().await {
                push(res, &mut results, &mut id_to_name);
            }
        })
        .await
        .is_ok()
        {
            return results;
        }

        // We tried to wait gracefully, abort the remaining tasks
        set.abort_all();

        if tokio::time::timeout(wait_timeout, async {
            while let Some(res) = set.join_next_with_id().await {
                push(res, &mut results, &mut id_to_name);
            }
        })
        .await
        .is_ok()
        {
            return results;
        }

        for (k, _) in id_to_name.into_values() {
            results.push((k, Err(eyre::eyre!("task failed to abort within time"))));
        }

        results
    }
}
