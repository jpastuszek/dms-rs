use std::sync::mpsc::channel;
use sender::Collector;

#[derive(Clone)]
pub enum ProducerEvent {
    Hello
}

mod probe;

pub fn start(collector: Collector) -> () {
    let (probe_notify, events) = channel();

    let probe = probe::start(collector.clone(), events);

    probe_notify.send(ProducerEvent::Hello).expect("probe thread died");

    probe.join().unwrap();
}

