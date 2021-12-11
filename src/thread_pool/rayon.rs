use rayon::ThreadPoolBuilder;

use crate::thread_pool::ThreadPool;
use crate::{KvsError, Result};

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(num: u32) -> Result<Self> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num as usize)
            .build()
            .map_err(|e| KvsError::StringError(format!("{}", e)))?;

        Ok(RayonThreadPool { pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
