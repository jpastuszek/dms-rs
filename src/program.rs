use flexi_logger::{init as log_init, LogConfig, LogRecord};
use time;
use std::process::exit;

fn app_format(record: &LogRecord) -> String {
    //2016-02-09 10:20:15,784 [myprog] INFO  src/myprog.rs - Processing update events
    let tm = time::at(time::get_time());
    let time: String = time::strftime("%Y-%m-%d %H:%M:%S,%f", &tm).unwrap();
    format!( "{} [{}] {:<5} {} - {}",
             &time[..time.len() - 6],
             record.location().module_path(),
             record.level(),
             record.location().file(),
             &record.args())
}

pub fn init<S: Into<String>>(spec: Option<S>) {
    let mut log_config = LogConfig::new();
    log_config.format = app_format;
    log_init(log_config, spec.map(|s| s.into())).unwrap();
}

pub fn exit_with_error(msg: String, code: i32) -> ! {
    error!("Exiting: {}", msg);
    exit(code);
}
