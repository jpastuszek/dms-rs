use std::thread::{self, JoinHandle};
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};
use std::error::Error;
use std::fmt;

use nanomsg::{Socket, Protocol, Error as NanoError};
use url::Url;
use chrono::{DateTime, UTC};

use messaging::*;

#[derive(Debug)]
pub enum SenderError {
    Connection(NanoError),
    Configuration(NanoError),
    Transport(NanoError)
}

impl From<NanoError> for SenderError {
    fn from(err: NanoError) -> SenderError {
        match err {
            NanoError::ProtocolNotSupported => SenderError::Configuration(err),
            NanoError::ProtocolNotAvailable => SenderError::Configuration(err),
            NanoError::AddressFamilyNotSupported => SenderError::Configuration(err),
            NanoError::ConnectionRefused => SenderError::Connection(err),
            NanoError::AddressInUse => SenderError::Connection(err),
            NanoError::NetworkDown => SenderError::Connection(err),
            NanoError::NetworkUnreachable => SenderError::Connection(err),
            NanoError::HostUnreachable => SenderError::Connection(err),
            NanoError::ConnectionReset => SenderError::Connection(err),
            NanoError::TimedOut => SenderError::Connection(err),
            _ => SenderError::Transport(err)
        }
    }
}

impl Error for SenderError {
    fn description(&self) -> &str {
        match self {
            &SenderError::Connection(_) => "Processor connecitivity issue",
            &SenderError::Configuration(_) => "Sender configuration error",
            &SenderError::Transport(_) => "Transport error",
        }
    }
}

impl fmt::Display for SenderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SenderError::Connection(err) => write!(f, "{}: {}", self.description(), err),
            &SenderError::Configuration(err) => write!(f, "{}: {}", self.description(), err),
            &SenderError::Transport(err) => write!(f, "{}: {}", self.description(), err),
        }
    }
}

pub struct Sender {
    sink: SyncSender<Box<RawDataPoint>>,
    thread: JoinHandle<()>
}

impl Sender {
    pub fn spawn(processor_url: Url) -> Result<Sender, SenderError> {
        let (tx, rx): (SyncSender<Box<RawDataPoint>>, Receiver<Box<RawDataPoint>>) = sync_channel(1000);

        let mut socket = try!(Socket::new(Protocol::Push));
        info!("Using processor URL: {}", &processor_url);
        let mut _endpoint = try!(socket.connect(&processor_url.serialize()[..]));

        let thread = thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(raw_data_point) => {
                        if let Err(err) = socket.send_message("".to_string(), *raw_data_point, Encoding::Capnp) {
                            error!("Failed to send raw data point to data processor at '{}': {}", &processor_url, err);
                        }
                    },
                    Err(error) => {
                        info!("Sender thread finished: {}", error);
                        return;
                    }
                }
            }
        });

        Ok(Sender {
            sink: tx,
            thread: thread
        })
    }

    pub fn stop(self) {
        let Sender {sink, thread} = self;
        info!("Stopping sender...");
        drop(sink);
        thread.join().unwrap();
        info!("Sender done");
    }

    pub fn collector(&self) -> Collector {
        Collector {
            timestamp: UTC::now(),
            sink: self.sink.clone(),
        }
    }
}

pub trait Collect {
    fn collect(&mut self, location: &str, path: &str, component: &str, value: DataValue) -> ();
}

pub struct Collector {
    timestamp: DateTime<UTC>,
    sink: SyncSender<Box<RawDataPoint>>,
}

impl Clone for Collector {
    fn clone(&self) -> Self {
        Collector {
            timestamp: self.timestamp,
            sink: self.sink.clone()
        }
    }
}

impl Collect for Collector {
    fn collect(&mut self, location: &str, path: &str, component: &str, value: DataValue) -> () {
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
    pub use std::io::Read;
    pub use std::error::Error;
    pub use messaging::*;
    pub use nanomsg::{Socket, Protocol};
    pub use url::Url;

    mod sender {
        pub use super::*;
        #[test]
        fn should_shut_down_after_going_out_of_scope() {
            {
                let _ = Sender::spawn(Url::parse("ipc:///tmp/test-collector1.ipc").unwrap());
            }
            assert!(true);
        }

        #[test]
        fn should_fail_to_spawn_on_bad_url() {
            let result =  Sender::spawn(Url::parse("foo:///bar").unwrap());
            assert!(result.is_err());
            if let Err(err) = result {
                assert_eq!(err.description(), "Sender configuration error");
            }
        }

        mod collector {
            pub use super::*;

            #[test]
             fn should_pass_data_points_to_nanosmg_pull_socket() {
                let mut pull = Socket::new(Protocol::Pull).unwrap();
                let mut _endpoint = pull.bind("ipc:///tmp/test-collector.ipc").unwrap();
                {
                    let sender = Sender::spawn(Url::parse("ipc:///tmp/test-collector.ipc").unwrap()).unwrap();
                    let mut collector = sender.collector();

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
            /*
            #[test]
            fn collect_should_fail_if_sender_paniced() {
                let sender = Sender::spawn(Url::parse("foo:///bar").unwrap()).unwrap();
                let mut collector = sender.collector();

                collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
            }
            */
        }
    }
}

