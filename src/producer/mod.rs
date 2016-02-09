use carboxyl::Sink;

use collector::Collector;

#[derive(Clone)]
pub enum ProducerEvent {
    Shutdown
}

mod probe;

pub fn start(collector: Collector) -> () {
    //TODO: shutdown trigger somehow
    let event_sink = Sink::new();

    let probe = probe::start(collector.clone(), event_sink.stream());
    probe.join().unwrap();
}


