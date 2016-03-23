use std::sync::mpsc::channel;
use sender::Collector;
use chan_signal::{self, Signal};

#[derive(Clone)]
pub enum ProducerEvent {
    Hello,
    Shutdown
}

mod probe;

pub fn start(collector: Collector) -> () {
    //TODO: move to main prog?
    //TODO: does not work on MacOS? does it work at all?
    let signals = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    let (probe_notify, events) = channel();
    let probe = probe::start(collector.clone(), events);
    probe_notify.send(ProducerEvent::Hello).expect("probe thread died");

    info!("Waiting for signals...");
    let signal = signals.recv().expect("chan_signal thread died");

    info!("Received signal: {:?}; shutting down", signal);
    probe_notify.send(ProducerEvent::Shutdown).ok();

    probe.join().unwrap();
}

