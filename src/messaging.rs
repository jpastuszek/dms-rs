use chrono::*;
use nanomsg::Socket;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::io::OutputStream;
use std::io::{Error,ErrorKind};

#[derive(Clone,Copy,Debug,PartialEq)]
pub enum DataType {
    RawDataPoint
}

#[derive(Clone,Copy,Debug,PartialEq)]
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

#[derive(Debug)]
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
                        let mut value_builder = raw_data_point_builder.borrow().init_value();
                        match self.value {
                            DataValue::Integer(value) => {
                                value_builder.set_integer(value);
                            },
                            DataValue::Float(value) => {
                                value_builder.set_float(value);
                            },
                            DataValue::Bool(value) => {
                                value_builder.set_boolean(value);
                            },
                            DataValue::Text(ref value) => {
                                value_builder.set_text(&*value);
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
    fn from_bytes(bytes: Vec<u8>) -> Result<MessageHeader, Error> {
        let splits = bytes.split(|byte| *byte == '\n' as u8);
        let mut parts = splits.take_while(|split| **split != []); // [] => \n\n

        let data_type;
        let topic;
        match parts.next() {
            Some(dt_topic) => {
                let mut splits = dt_topic.splitn(2, |byte| *byte == '/' as u8);
                data_type = match splits.next() {
                    Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                        Ok(string) => match &*string {
                            "RawDataPoint" => DataType::RawDataPoint,
                            _ => return Err(Error::new(ErrorKind::InvalidInput, &*format!("unknown data type: {}", string)))
                        },
                        Err(utf8_error) => return Err(Error::new(ErrorKind::InvalidInput, &*format!("error decoding data type string: {}", utf8_error)))
                    },
                    None => return Err(Error::new(ErrorKind::InvalidInput, "no data type found in message header"))
                };
                topic = match splits.next() {
                    Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                        Ok(string) => string,
                        Err(utf8_error) => return Err(Error::new(ErrorKind::InvalidInput, &*format!("error decoding topic string: {}", utf8_error)))
                    },
                    None => return Err(Error::new(ErrorKind::InvalidInput, "no topic found in message header"))
                };
            },
            None => return Err(Error::new(ErrorKind::InvalidInput, "no data type/topic part found in message header"))
        };

        let version = match parts.next() {
            Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                Ok(string) => match string.parse::<u8>() {
                    Ok(int) => int,
                    Err(parse_error) => return Err(Error::new(ErrorKind::InvalidInput, &*format!("message version is not u8 number: {}", parse_error)))
                },
                Err(utf8_error) => return Err(Error::new(ErrorKind::InvalidInput, &*format!("error decoding version string: {}", utf8_error)))
            },
            None => return Err(Error::new(ErrorKind::InvalidInput, "no version found in message header"))
        };

        let encoding = match parts.next() {
            Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                Ok(string) => match &*string {
                    "capnp" => Encoding::Capnp,
                    _ => return Err(Error::new(ErrorKind::InvalidInput, &*format!("unknown encoding: {}", string)))
                },
                Err(utf8_error) => return Err(Error::new(ErrorKind::InvalidInput, &*format!("error decoding encoding string: {}", utf8_error)))
            },
            None => return Err(Error::new(ErrorKind::InvalidInput, "no encoding found in message header"))
        };

        for part in parts {
            warn!("found extra part in message header: {:?}", part);
        };

        Ok(MessageHeader {
            data_type: data_type,
            topic: topic,
            version: version,
            encoding: encoding
        })
    }

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

    describe! message_header {
        it "should be serializable" {
            let header = MessageHeader {
                data_type: DataType::RawDataPoint,
                topic: "hello".to_string(),
                version: 42,
                encoding: Encoding::Capnp
            };

            let bytes = header.to_bytes();
            assert_eq!(bytes, "RawDataPoint/hello\n42\ncapnp\n\n".to_string().into_bytes());
        }

        it "should be deserializable" {
            let bytes = "RawDataPoint/hello\n42\ncapnp\n\n".to_string().into_bytes();

            let header = MessageHeader::from_bytes(bytes).unwrap();
            assert_eq!(header.data_type, DataType::RawDataPoint);
            assert_eq!(header.topic, "hello".to_string());
            assert_eq!(header.version, 42);
            assert_eq!(header.encoding, Encoding::Capnp);
        }
    }

    describe! nanomsg_socket_extensions {
        describe! send_message {
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
                    Ok(_) => {
                        //println!("{:?}", msg);
                        let mut splits = msg.splitn(5, |byte| *byte == '\n' as u8);
                        assert_eq!(splits.next().unwrap(), &*"RawDataPoint/hello".to_string().into_bytes());
                        assert_eq!(splits.next().unwrap(), &*"0".to_string().into_bytes());
                        assert_eq!(splits.next().unwrap(), &*"capnp".to_string().into_bytes());
                        assert!(splits.next().unwrap().is_empty()); // body separator
                        assert!(splits.next().unwrap().len() > 10); // body
                        assert!(splits.next().is_none()); // end
                    },
                    Err(error) => panic!("got error: {}", error)
                }
            }
        }
    }
}

