//! Private owned operation lifetime, separate from the Python completion bridge.
use std::future::Future;
use tokio::runtime::Handle;

pub(crate) fn prepare_owned<F>(operation: F) -> (
    impl Future<Output = Result<F::Output, String>> + Send,
    impl FnOnce(&Handle),
)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let bridge = async move {
        receiver.await.map_err(|_| "Owned operation ended without a completion".to_owned())
    };
    let start = move |runtime: &Handle| {
        // Detached deliberately: caller cancellation must not cancel a sent mutation.
        runtime.spawn(async move {
            let result = operation.await;
            let _ = sender.send(result);
        });
    };
    (bridge, start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn owned_start_does_not_wait_for_completion_bridge_poll() {
        let (entered, observed) = oneshot::channel();
        let (bridge, start) = prepare_owned(async move { let _ = entered.send(()); 17 });
        start(&Handle::current());
        tokio::time::timeout(Duration::from_secs(1), observed).await
            .expect("owned request must not wait for the Python bridge task").unwrap();
        assert_eq!(bridge.await.unwrap(), 17);
    }

    #[tokio::test]
    async fn uncommitted_bridge_failure_cannot_start_operation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let (bridge, start) = prepare_owned(async move { counter.fetch_add(1, Ordering::SeqCst); });
        drop(bridge); drop(start);
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn caller_cancellation_keeps_submitted_operation_owned_exactly_once() {
        let (entered, observed) = oneshot::channel();
        let (finish, wait) = oneshot::channel();
        let (done, completed) = oneshot::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let (bridge, start) = prepare_owned(async move {
            let _ = entered.send(());
            wait.await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            let _ = done.send(());
        });
        start(&Handle::current());
        tokio::time::timeout(Duration::from_secs(1), observed).await.unwrap().unwrap();
        drop(bridge); finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), completed).await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn operation_error_is_not_changed_or_retried() {
        let (bridge, start) = prepare_owned(async { Err::<(), _>("original operation error") });
        start(&Handle::current());
        assert_eq!(bridge.await.unwrap(), Err("original operation error"));
    }

    #[tokio::test]
    async fn operation_panic_closes_completion_instead_of_hanging() {
        let (bridge, start) = prepare_owned(async { panic!("synthetic owned operation panic") });
        start(&Handle::current());
        assert!(tokio::time::timeout(Duration::from_secs(1), bridge).await.unwrap().is_err());
    }
}
