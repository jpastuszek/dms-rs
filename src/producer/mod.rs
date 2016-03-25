use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Receiver};
use program::Signal;
use sender::Collector;

mod probe;

pub fn spawn(collector: Collector, signals: Receiver<Signal>) -> JoinHandle<()> {
    thread::spawn(move || {
        let (probe_signal, probe_signals) = channel();
        let probe = probe::spawn(probe_signals, collector.clone());

        loop {
            match signals.recv() {
                Ok(signal) => {
                    info!("Received signal: {:?}", signal);
                    probe_signal.send(signal).expect("probe module died");
                }
                Err(_) => {
                    drop(probe_signal);
                    probe.join().ok();
                    break
                }
            }
        }

        info!("Producer done");
    })
}

