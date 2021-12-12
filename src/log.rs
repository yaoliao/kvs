use log::LevelFilter;

pub struct KvsLog;

impl KvsLog {
    pub fn log_setting() {
        use chrono::Local;
        use std::io::Write;

        // 设置日志
        let env = env_logger::Env::default()
            .filter_or(env_logger::DEFAULT_FILTER_ENV, LevelFilter::Debug.as_str());
        env_logger::Builder::from_env(env)
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} {} [{}:{}] {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.module_path().unwrap_or("<unnamed>"),
                    record.line().unwrap_or(0),
                    &record.args()
                )
            })
            .init();
    }
}
