#[derive(Clone)]
pub enum ProducerEvent {
    Shutdown
}

mod probe;

/*
pub fn start<S>(data_processor_address: S) -> CollectorThread where S: Into<String> {
    let collector_thread = CollectorThread::spawn(data_processor_address);

    let probe = probe::start(collector_thread.new_collector());
    //TODO: register modules
    collector_thread.add(probe);

    collector_thread
}
*/


