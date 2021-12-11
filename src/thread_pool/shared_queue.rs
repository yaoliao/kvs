use crossbeam::channel;
use log::{debug, error};

use crate::thread_pool::ThreadPool;
use crate::Result;

type BoxFn = Box<dyn FnOnce() + Send + 'static>;

pub struct SharedQueueThreadPool {
    sender: channel::Sender<BoxFn>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(num: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (s, r) = channel::unbounded::<BoxFn>();

        for _ in 0..num {
            let rec = TaskReceiver(r.clone());
            std::thread::spawn(|| run_tasks(rec));
        }

        Ok(SharedQueueThreadPool { sender: s })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Box::new(job))
            .expect("The thread pool has no thread.");
    }
}

#[derive(Clone)]
struct TaskReceiver(channel::Receiver<BoxFn>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let rec = TaskReceiver(self.0.clone());
            if let Err(e) = std::thread::Builder::new().spawn(move || run_tasks(rec)) {
                error!("Failed to spawn a thread: {}", e);
            }
        }
    }
}

fn run_tasks(rec: TaskReceiver) {
    loop {
        if let Ok(job) = rec.0.recv() {
            job();
        } else {
            debug!("Thread exits because the thread pool is destroyed.");
        }
    }
}
