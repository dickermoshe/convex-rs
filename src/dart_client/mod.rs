use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        Arc,
        Mutex,
    },
};

#[cfg(debug_assertions)]
use android_logger::Config;
use async_once_cell::OnceCell;
use flutter_rust_bridge::{
    frb,
    DartFnFuture,
};
use futures::{
    channel::oneshot::{
        self,
        Sender,
    },
    pin_mut,
    select_biased,
    FutureExt,
    StreamExt,
};
use log::debug; // Logging for debugging purposes
#[cfg(debug_assertions)]
use log::LevelFilter;

use crate::{
    ConvexClient,
    ConvexClientBuilder,
    ConvexError,
    FunctionResult,
    Value,
};

/// A wrapper around a [ConvexClient] and a [tokio::runtime::Runtime] used to
/// asynchronously call Convex functions.
///
/// That enables easy async communication for mobile clients. They can call the
/// various methods on [MobileConvexClient] and await results without blocking
/// their main threads.

pub struct MobileConvexClient {
    deployment_url: String,
    client_id: String,
    client: OnceCell<ConvexClient>,
    rt: tokio::runtime::Runtime,
}

impl MobileConvexClient {
    /// Creates a new [MobileConvexClient].
    ///
    /// The internal [ConvexClient] doesn't get created/connected until the
    /// first public method call that hits the Convex backend.
    ///
    /// The `client_id` should be a string representing the name and version of
    /// the foreign client.
    #[frb(sync)]
    pub fn new(deployment_url: String, client_id: String) -> MobileConvexClient {
        #[cfg(debug_assertions)]
        android_logger::init_once(Config::default().with_max_level(LevelFilter::Error));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        MobileConvexClient {
            deployment_url,
            client_id,
            client: OnceCell::new(),
            rt,
        }
    }

    /// Returns a connected [ConvexClient].
    ///
    /// The first call is guaranteed to create the client object and subsequent
    /// calls will return clones of that connected client.
    ///
    /// Returns an error if ...
    /// TODO figure out reasons.
    async fn connected_client(&self) -> anyhow::Result<ConvexClient> {
        let url = self.deployment_url.clone();
        self.client
            .get_or_try_init(async {
                let client_id = self.client_id.to_owned();
                self.rt
                    .spawn(async move {
                        ConvexClientBuilder::new(url.as_str())
                            .with_client_id(&client_id)
                            .build()
                            .await
                    })
                    .await?
            })
            .await
            .map(|client_ref| client_ref.clone())
    }

    /// Executes a query on the Convex backend.
    #[frb]
    pub async fn query(
        &self,
        name: String,
        args: HashMap<String, Value>,
    ) -> Result<Value, ClientError> {
        let args = hashmap_to_btreemap(args);
        let mut client = self.connected_client().await?;
        debug!("got the client");
        let result = client.query(name.as_str(), args).await?;
        debug!("got the result");
        handle_direct_function_result(result)
    }

    /// Subscribe to updates to a query against the Convex backend.
    ///
    /// The [QuerySubscriber] will be called back with initial query results and
    /// it will continue to get called as the underlying data changes.
    ///
    /// The returned [SubscriptionHandle] can be used to cancel the
    /// subscription.
    #[frb]
    pub async fn subscribe(
        &self,
        name: String,
        args: HashMap<String, Value>,
        on_update: impl Fn(FunctionResult) -> DartFnFuture<()> + Send + Sync + 'static,
    ) -> Result<SubscriptionHandle, ClientError> {
        let subscriber = Arc::new(QuerySubscriber::new(Box::new(on_update)));
        self.internal_subscribe(name, args, subscriber)
            .await
            .map_err(Into::into)
    }

    async fn internal_subscribe(
        &self,
        name: String,
        args: HashMap<String, Value>,
        subscriber: Arc<QuerySubscriber>,
    ) -> anyhow::Result<SubscriptionHandle> {
        let args = hashmap_to_btreemap(args);
        let mut client = self.connected_client().await?;
        debug!("New subscription");
        let mut subscription = client.subscribe(name.as_str(), args).await?;
        let (cancel_sender, cancel_receiver) = oneshot::channel::<()>();
        self.rt.spawn(async move {
            let cancel_fut = cancel_receiver.fuse();
            pin_mut!(cancel_fut);
            loop {
                select_biased! {
                    new_val = subscription.next().fuse() => {
                        let new_val = new_val.expect("Client dropped prematurely");
                        // Instead of serializing the result to Dart, and calling
                        // specific on_error and on_update callbacks, we've directly
                        // pass the new event to the subscriber.
                        subscriber.on_update(new_val.into()).await;
                    }
                    _ = cancel_fut => {
                        break;
                    }
                }
            }
            debug!("Subscription canceled");
        });
        Ok(SubscriptionHandle::new(cancel_sender))
    }

    /// Executes a mutation on the Convex backend.
    #[frb]
    pub async fn mutation(
        &self,
        name: String,
        args: HashMap<String, Value>,
    ) -> Result<Value, ClientError> {
        let result = self.internal_mutation(name, args).await?;
        handle_direct_function_result(result)
    }

    /// Internal method for mutation logic.
    async fn internal_mutation(
        &self,
        name: String,
        args: HashMap<String, Value>,
    ) -> anyhow::Result<FunctionResult> {
        let args = hashmap_to_btreemap(args);
        let mut client = self.connected_client().await?;
        self.rt
            .spawn(async move { client.mutation(&name, args).await })
            .await?
    }

    /// Executes an action on the Convex backend.
    #[frb]
    pub async fn action(
        &self,
        name: String,
        args: HashMap<String, Value>,
    ) -> Result<Value, ClientError> {
        debug!("Running action: {}", name);
        let result = self.internal_action(name, args).await?;
        debug!("Got action result: {:?}", result);
        handle_direct_function_result(result)
    }

    async fn internal_action(
        &self,
        name: String,
        args: HashMap<String, Value>,
    ) -> anyhow::Result<FunctionResult> {
        let args = hashmap_to_btreemap(args);
        let mut client = self.connected_client().await?;
        debug!("Running action: {}", name);
        self.rt
            .spawn(async move { client.action(&name, args).await })
            .await?
    }

    /// Provide an OpenID Connect ID token to be associated with this client.
    ///
    /// Doing so will share that information with the Convex backend and a valid
    /// token will give the backend knowledge of a logged in user.
    ///
    /// Passing [None] for the token will disassociate a previous token,
    /// effectively returning to a logged out state.
    #[frb]
    pub async fn set_auth(&self, token: Option<String>) -> anyhow::Result<()> {
        Ok(self.internal_set_auth(token).await?)
    }

    /// Internal method for setting authentication.
    async fn internal_set_auth(&self, token: Option<String>) -> anyhow::Result<()> {
        let mut client = self.connected_client().await?;
        self.rt
            .spawn(async move { client.set_auth(token).await })
            .await
            .map_err(|e| e.into())
    }
}

/// Error types from Convex operations.
///
/// Convex functions handle errors in three ways:
/// - Return different values for expected errors (e.g., validation failures)
/// - Throw ConvexError for application-specific errors with structured data
/// - Let unexpected errors become generic server errors
#[derive(Debug, thiserror::Error)]
#[frb]
pub enum ClientError {
    /// Client-side error (network, serialization, etc.).
    #[error("InternalError: {msg}")]
    InternalError {
        /// Error message describing the client-side issue.
        msg: String,
    },
    /// Application error from Convex functions using ConvexError.
    ///
    /// Contains structured error data for expected error scenarios like
    /// validation failures or business logic constraints.
    #[error("ConvexError: {err}")]
    ConvexError {
        /// The ConvexError with application-specific error details and data.
        err: ConvexError,
    },
    /// Unexpected server-side error from Convex functions.
    #[error("ServerError: {msg}")]
    ServerError {
        /// Generic error message from the server.
        msg: String,
    },
}

/// Utility function to handle and serialize FunctionResult into a string or
/// error.

fn handle_direct_function_result(result: FunctionResult) -> Result<Value, ClientError> {
    match result {
        FunctionResult::Value(v) => Ok(v.into()),
        FunctionResult::ConvexError(e) => Err(ClientError::ConvexError { err: e.into() }),
        FunctionResult::ErrorMessage(msg) => Err(ClientError::ServerError { msg }),
    }
}

fn hashmap_to_btreemap(hashmap: HashMap<String, Value>) -> BTreeMap<String, Value> {
    hashmap.into_iter().collect()
}

/// Handle for managing query subscriptions.
#[frb]
pub struct SubscriptionHandle {
    // flutter_rust_bridge does not work well with Arc<...> types, so we've
    // moved the Arc<...> inside the struct instead. We can then return
    // an instance of SubscriptionHandle instead of an Arc<SubscriptionHandle>.
    cancel_sender: Arc<Mutex<Option<Sender<()>>>>,
}

impl SubscriptionHandle {
    fn new(cancel_sender: Sender<()>) -> Self {
        SubscriptionHandle {
            cancel_sender: Arc::new(Mutex::new(Some(cancel_sender))),
        }
    }

    /// Cancels the subscription by sending a cancellation signal.
    #[frb(sync)]
    pub fn cancel(&self) {
        if let Some(sender) = self.cancel_sender.lock().unwrap().take() {
            sender.send(()).unwrap();
        }
    }
}

impl From<anyhow::Error> for ClientError {
    fn from(value: anyhow::Error) -> Self {
        Self::InternalError {
            msg: value.to_string(),
        }
    }
}

struct QuerySubscriber {
    on_update: Box<dyn Fn(FunctionResult) -> DartFnFuture<()> + Send + Sync>,
}

impl QuerySubscriber {
    fn new(on_update: Box<dyn Fn(FunctionResult) -> DartFnFuture<()> + Send + Sync>) -> Self {
        QuerySubscriber { on_update }
    }

    async fn on_update(&self, value: FunctionResult) {
        (self.on_update)(value).await;
    }
}
