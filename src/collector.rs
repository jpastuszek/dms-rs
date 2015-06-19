use std::thread;
use std::thread::JoinGuard;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};

use nanomsg::{Socket, Protocol};
use chrono::{DateTime, UTC};

use messaging::*;

pub struct CollectorThread<'a> {
    // NOTE: this needs to be before thread so channel is dropped before we join tread
    sink: SyncSender<Box<RawDataPoint>>,
    // NOTE: this is actually needed to join the thread after SyncSender is closed
    #[allow(dead_code)]
    thread: JoinGuard<'a, ()>,
}

impl<'a> CollectorThread<'a> {
    pub fn spawn(data_processor_address: &'a str) -> CollectorThread<'a> {
        let (tx, rx): (SyncSender<Box<RawDataPoint>>, Receiver<Box<RawDataPoint>>) = sync_channel(1000);

        let thread = thread::scoped(move || {
            let mut socket = Socket::new(Protocol::Push).ok().expect("Cannot create push socket!");
            let mut _endpoint = match socket.connect(data_processor_address) {
                Ok(ep) => ep,
                Err(error) => panic!("Failed to connect data processor at '{}': {}", data_processor_address, error)
            };

            loop {
                match rx.recv() {
                    Ok(raw_data_point) => {
                        socket.send_message("".to_string(), *raw_data_point, Encoding::Capnp).unwrap();
                    },
                    Err(error) => {
                        info!("Collector thread finished: {}", error);
                        return ();
                    }
                }
            }
        });

        CollectorThread {
            thread: thread,
            sink: tx
        }
    }

    pub fn new_collector(& self) -> Collector {
        Collector {
            timestamp: UTC::now(),
            sink: self.sink.clone(),
        }
    }
}

pub struct Collector {
    timestamp: DateTime<UTC>,
    sink: SyncSender<Box<RawDataPoint>>,
}

impl Collector {
    pub fn collect(&mut self, location: &str, path: &str, component: &str, value: DataValue) -> () {
        let raw_data_point = Box::new(RawDataPoint {
            location: location.to_string(),
            path: path.to_string(),
            component: component.to_string(),
            timestamp: self.timestamp,
            value: value
        });

        match self.sink.send(raw_data_point) {
            Ok(_) => {
                debug!("Collected raw data point for location: '{}', path: '{}', component: '{}'", location, path, component);
            }
            Err(error) => {
                error!("Failed to send collected raw data point: {}", error);
            }
        }
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use messaging::*;
    pub use nanomsg::{Socket, Protocol};
    pub use std::io::Read;

    describe! collector_thread {
        it "should shut down after going out of scope" {
            {
                let _ = CollectorThread::spawn("ipc:///tmp/test-collector1.ipc");
            }
            assert!(true);
        }

        describe! collector {
             it "should pass data points to nanosmg pull socket" {
                let mut pull = Socket::new(Protocol::Pull).unwrap();
                let mut _endpoint = pull.bind("ipc:///tmp/test-collector.ipc").unwrap();
                {
                    let collector_thread = CollectorThread::spawn("ipc:///tmp/test-collector.ipc");
                    let mut collector = collector_thread.new_collector();

                    collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
                    collector.collect("foobar", "os/cpu/sys", "user", DataValue::Float(0.4));

                    let mut msg = Vec::new();
                    pull.read_to_end(&mut msg).unwrap();
                    let msg_string = String::from_utf8_lossy(&msg);
                    assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
                    assert!(msg_string.contains("myserver"));

                    let mut msg = Vec::new();
                    pull.read_to_end(&mut msg).unwrap();
                    let msg_string = String::from_utf8_lossy(&msg);
                    assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
                    assert!(msg_string.contains("foobar"));
                }
            }
        }
    }
}
