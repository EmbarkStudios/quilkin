use tokio_util::sync::CancellationToken as Token;
pub use tokio_util::task::TaskTracker;

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

            /// <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html#method.cancel>
            #[inline]
            pub fn cancel(&self) {
                self.0.cancel();
            }

            /// <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html#method.is_cancelled>
            #[inline]
            pub fn is_cancelled(&self) -> bool {
                self.0.is_cancelled()
            }

            /// <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html#method.cancelled>
            #[inline]
            pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
                self.0.cancelled()
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
    };
}

common!(RootToken);
common!(ChildToken);

pub struct GracefulSpawner {
    tracker: TaskTracker,
    token: Token,
}

impl From<RootToken> for GracefulSpawner {
    fn from(value: RootToken) -> Self {
        Self {
            tracker: TaskTracker::new(),
            token: value.0,
        }
    }
}

impl From<ChildToken> for GracefulSpawner {
    fn from(value: ChildToken) -> Self {
        Self {
            tracker: TaskTracker::new(),
            token: value.0,
        }
    }
}

impl GracefulSpawner {
    #[inline]
    pub fn handle(&mut self) -> Spawner<'_> {
        Spawner { gs: self }
    }

    #[inline]
    pub fn token(&self) -> Token {
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
}

pub struct Spawner<'gs> {
    gs: &'gs GracefulSpawner,
}

impl Spawner<'_> {
    /// Spawns a task tracked by this spawner, which will wait for all spawned tasks
    #[inline]
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.gs.tracker.spawn(task)
    }

    #[inline]
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.gs.token.cancelled()
    }

    #[inline]
    pub fn token(&self) -> Token {
        self.gs.token.clone()
    }
}
