use chrono::*;
use nanomsg::Socket;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::io::OutputStream;
use std::io::Error;

#[derive(Debug)]
pub enum DataType {
    RawDataPoint
}

#[derive(Clone,Copy,Debug)]
pub enum Encoding {
    Capnp
}

pub trait SerDeMessage {
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, Error>;
    fn data_type() -> DataType;
    fn version() -> u8 {
        0
    }
}

#[derive(Clone,Debug)]
#[allow(dead_code)]
pub enum DataValue {
    Integer(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

#[derive(Debug)]
pub struct RawDataPoint {
    pub location: String,
    pub path: String,
    pub component: String,
    pub timestamp: DateTime<UTC>,
    pub value: DataValue,
}

impl SerDeMessage for RawDataPoint {
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, Error> {
        match encoding {
            Encoding::Capnp => {
                let mut message = MallocMessageBuilder::new_default();
                {
                    let mut raw_data_point_builder = message.init_root::<super::raw_data_point_capnp::raw_data_point::Builder>();

                    raw_data_point_builder.set_location(&*self.location);
                    raw_data_point_builder.set_path(&*self.path);
                    raw_data_point_builder.set_component(&*self.component);

                    {
                        let mut date_time_builder = raw_data_point_builder.borrow().init_timestamp();
                        date_time_builder.set_unix_timestamp(self.timestamp.timestamp());
                        date_time_builder.set_nanosecond(self.timestamp.nanosecond());
                    }

                    {
                        let mut _value = raw_data_point_builder.borrow().init_value();
                        match self.value {
                            DataValue::Integer(value) => {
                                _value.set_integer(value);
                            },
                            DataValue::Float(value) => {
                                _value.set_float(value);
                            },
                            DataValue::Bool(value) => {
                                _value.set_boolean(value);
                            },
                            DataValue::Text(ref value) => {
                                _value.set_text(&*value);
                            },
                        }
                    }
                }

                let mut data = Vec::new();
                match serialize_packed::write_packed_message_unbuffered(&mut data, &mut message) {
                    Ok(_) => {
                        trace!("Message serialized ({} bytes)", data.len());
                        return Ok(data);
                    },
                    Err(error) => {
                        error!("Failed to serialize message for {:?}: {}", <RawDataPoint as SerDeMessage>::data_type(), error);
                        return Err(error);
                    }
                }
            }
        }
    }

    fn data_type() -> DataType {
        DataType::RawDataPoint
    }
}

#[derive(Debug)]
pub struct MessageHeader {
    data_type: DataType,
    topic: String,
    version: u8,
    encoding: Encoding,
}

impl MessageHeader {
    fn to_bytes(&self) -> Vec<u8> {
        let encoding = match self.encoding {
            Encoding::Capnp => "capnp"
        };
        let data_type = match self.data_type {
            DataType::RawDataPoint => "RawDataPoint"
        };
        format!("{}/{}\n{}\n{}\n\n", data_type, self.topic, self.version, encoding).into_bytes()
    }
}

pub trait SendMessage<T> {
    fn send_message(&mut self, topic: String, message: T, encoding: Encoding) -> Result<(), Error> where T: SerDeMessage;
}

impl<T> SendMessage<T> for Socket {
    fn send_message(&mut self, topic: String, message: T, encoding: Encoding) -> Result<(), Error>
        where T: SerDeMessage {
        let mut data: Vec<u8>;

        let header = MessageHeader {
            data_type: T::data_type(),
            topic: topic,
            version: T::version(),
            encoding: encoding
        };
        data = header.to_bytes();

        let body = try!(message.to_bytes(encoding));

        data.extend(body);

        debug!("Sending message with {:?} on topic {}", T::data_type(), header.topic);
        match self.write(&data) {
            Ok(_) => trace!("Message sent"),
            Err(error) => {
                error!("Failed to send message: {}", error);
                return Err(error);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod test {
    pub use super::*;

    pub use nanomsg::{Socket, Protocol};
    pub use capnp::{MessageBuilder, MallocMessageBuilder};
    pub use std::thread;
    pub use std::io::Read;
    pub use chrono::*;

    #[allow(dead_code)]
    pub mod raw_data_point_capnp {
        include!("./schema/raw_data_point_capnp.rs");
    }

    describe! nanomsg_socket_extensions {
        it "should allow sending message with header and capnp serialized body" {
            let mut pull = Socket::new(Protocol::Pull).unwrap();
            let mut _endpoint = pull.bind("ipc:///tmp/test.ipc").unwrap();

            let _ = thread::scoped(move || {
                let mut socket = Socket::new(Protocol::Push).unwrap();
                let mut _endpoint = socket.connect("ipc:///tmp/test.ipc").unwrap();
                let now = UTC::now();

                let message = RawDataPoint {
                    location: "myserver".to_string(),
                    path: "cpu/usage".to_string(),
                    component: "iowait".to_string(),
                    timestamp: now,
                    value: DataValue::Float(0.2)
                };

                socket.send_message("hello".to_string(), message, Encoding::Capnp).unwrap();
            });

            let mut msg = Vec::new();
            match pull.read_to_end(&mut msg) {
                Ok(_) => println!("{:?}", msg),
                Err(error) => panic!("got error: {}", error)
            }
        }
    }
}

