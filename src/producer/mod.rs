use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Receiver};
use chan_signal::Signal;
use sender::Collector;

#[derive(Clone)]
pub enum ProducerEvent {
    Hello,
    Shutdown
}

mod probe;

pub fn spawn(collector: Collector, signals: Receiver<Signal>) -> JoinHandle<()> {
    thread::spawn(move || {
        let (probe_notify, probe_events) = channel();
        let probe = probe::start(collector.clone(), probe_events);
        probe_notify.send(ProducerEvent::Hello).expect("probe thread died");

        let signal = signals.recv().expect("main thread died");

        info!("Received signal: {:?}: shutting down...", signal);
        probe_notify.send(ProducerEvent::Shutdown).ok();

        probe.join().unwrap();

        info!("Producer done");
    })
}

