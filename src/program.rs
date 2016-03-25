use std::process::exit;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
pub use std::thread::JoinHandle;
use flexi_logger::{init as log_init, LogConfig, LogRecord};
use time;
use chan_signal::{notify, Signal as Sig};

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

#[derive(Clone, Debug)]
pub enum Signal {
    Reload
}

pub fn init<S: Into<String>>(spec: Option<S>) -> Receiver<Signal> {
    //NOTE: this has to be called before any thread is spawned
    let chan_signals = notify(&[Sig::INT, Sig::TERM, Sig::HUP]);

    let mut log_config = LogConfig::new();
    log_config.format = app_format;
    log_init(log_config, spec.map(|s| s.into())).unwrap();

    let (signal, signals) = channel();

    let _ = thread::spawn(move || {
        debug!("Waiting for signals...");
        loop {
            let sig = chan_signals.recv().expect("chan_signal thread died");
            info!("Process received OS signal: {:?}", sig);
            signal.send(match sig {
                Sig::HUP => Signal::Reload,
                _ => return
            }).expect("main thread died?!");
        }
    });

    signals
}

pub fn exit_with_error(msg: String, code: i32) -> ! {
    error!("Exiting: {}", msg);
    exit(code);
}

pub fn spawn<F, T>(name: &str, f: F) -> JoinHandle<T> where F: FnOnce() -> T, F: Send + 'static, T: Send + 'static {
    thread::Builder::new().name(name.to_string()).spawn(f).expect("failed to spawn program thread")
}

