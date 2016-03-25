use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Receiver};
use program::Signal;
use sender::Collector;

mod probe;

pub fn spawn(collector: Collector, signals: Receiver<Signal>) -> JoinHandle<()> {
    thread::spawn(move || {
        let (probe_signal, probe_signals) = channel();
        let probe = probe::spawn(probe_signals, collector.clone());

        let signal = signals.recv().expect("main thread died");

        info!("Received signal: {:?}", signal);

        probe_signal.send(signal).unwrap();
        probe.join().unwrap();

        info!("Producer done");
    })
}

