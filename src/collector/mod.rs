//use nanomsg::{Socket, NanoResult, Protocol};
use chrono::*;

use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::io::WriteOutputStream;

use std::io::*;

use std::thread;
use std::thread::JoinGuard;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};

#[derive(Clone,Debug)]
#[allow(dead_code)]
pub enum DataValue {
    Integer(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

#[derive(Debug)]
struct RawDataPoint {
    location: String,
    path: String,
    component: String,
    timestamp: DateTime<UTC>,
    value: DataValue,
}


pub struct CollectorThread<'a> {
    // NOTE: this needs to be before thread so channel is dropped before we join tread
    sink: SyncSender<Box<RawDataPoint>>,
    // NOTE: this is actually needed to join the thread after SyncSender is closed
    #[allow(dead_code)]
    thread: JoinGuard<'a, ()>,
}

impl<'a> CollectorThread<'a> {
    pub fn spawn() -> CollectorThread<'a> {
        let (tx, rx): (SyncSender<Box<RawDataPoint>>, Receiver<Box<RawDataPoint>>) = sync_channel(1000);

        let thread = thread::scoped(move || {
            loop {
                match rx.recv() {
                    Ok(raw_data_point) => {
                        let mut message = MallocMessageBuilder::new_default();
                        {
                            let mut raw_data_point_builder = message.init_root::<super::raw_data_point_capnp::raw_data_point::Builder>();

                            raw_data_point_builder.set_location(&*raw_data_point.location);
                            raw_data_point_builder.set_path(&*raw_data_point.path);
                            raw_data_point_builder.set_component(&*raw_data_point.component);

                            {
                                let mut date_time_builder = raw_data_point_builder.borrow().init_timestamp();
                                date_time_builder.set_unix_timestamp(raw_data_point.timestamp.timestamp());
                                date_time_builder.set_nanosecond(raw_data_point.timestamp.nanosecond());
                            }

                            {
                                let mut _value = raw_data_point_builder.borrow().init_value();
                                match raw_data_point.value {
                                    DataValue::Integer(value) => {
                                        _value.set_integer(value);
                                    },
                                    DataValue::Float(value) => {
                                        _value.set_float(value);
                                    },
                                    DataValue::Bool(value) => {
                                        _value.set_boolean(value);
                                    },
                                    DataValue::Text(value) => {
                                        _value.set_text(&*value);
                                    },
                                }
                            }
                        }

                        let mut out = WriteOutputStream::new(stdout());

                        serialize_packed::write_packed_message_unbuffered(&mut out, &mut message).ok().unwrap();
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
            value: value.clone()
        });

        match self.sink.send(raw_data_point) {
            Ok(_) => {
                debug!("Collected raw data point for location: '{}', path: '{}', component: '{}', value: {:?}", location, path, component, value);
            }
            Err(error) => {
                error!("Failed to send collected raw data point: {}", error);
            }
        }
    }
}

describe! collector_thread {
    it "should shut down after going out of scope" {
        {
            let _ = CollectorThread::spawn();
        }
        assert!(true);
    }

    describe! collector {
         it "should allow passing data points to collector threa" {
            let collector_thread = CollectorThread::spawn();

            let mut collector = collector_thread.new_collector();

            collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
            collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
        }
    }
}

