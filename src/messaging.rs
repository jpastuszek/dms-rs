use std::borrow::ToOwned;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error as IoError;
use std::fmt;
use std::error::Error;
use std::string::ToString;
use chrono::*;
use nanomsg::Socket;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder, MessageReader};
use capnp::message::ReaderOptions;
use capnp::io::OutputStream;
use capnp::io::ArrayInputStream;
use capnp::Error as CapnpError;

#[derive(Debug)]
enum SerDeErrorCause {
    CapnpError(CapnpError),
    IoError(IoError),
    Other(String),
    EncodingNotImplemented(Encoding),
}

impl Display for SerDeErrorCause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SerDeErrorCause::CapnpError(ref error) => write!(f, "Cap'n Proto Error: {}", error),
            &SerDeErrorCause::IoError(ref error) => write!(f, "IO Error: {}", error),
            &SerDeErrorCause::Other(ref msg) => write!(f, "{}", msg),
            &SerDeErrorCause::EncodingNotImplemented(ref enc) => write!(f, "encoding '{}' not implemented", enc.to_string()),
        }
    }
}

#[derive(Debug)]
struct DeserializationError<T> where T: SerDeMessage + Debug {
    pub cause: SerDeErrorCause,
    phantom: PhantomData<T>
}

#[derive(Debug)]
struct SerializationError<T> where T: SerDeMessage + Debug {
    pub cause: SerDeErrorCause,
    phantom: PhantomData<T>
}

impl<T: SerDeMessage + Debug> SerializationError<T> {
    fn not_implemented(encoding: Encoding) -> SerializationError<T> {
        SerializationError { cause: SerDeErrorCause::EncodingNotImplemented(encoding), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> DeserializationError<T> {
    fn not_implemented(encoding: Encoding) -> DeserializationError<T> {
        DeserializationError { cause: SerDeErrorCause::EncodingNotImplemented(encoding), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> Error for DeserializationError<T> {
    fn description(&self) -> &str {
        "deserialization error"
    }
}

impl<T: SerDeMessage + Debug> Error for SerializationError<T> {
    fn description(&self) -> &str {
        "serialization error"
    }
}

impl<T: SerDeMessage + Debug> fmt::Display for DeserializationError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to deserializae message for type {:?}: {}", T::data_type(), self.cause)
    }
}

impl<T: SerDeMessage + Debug> fmt::Display for SerializationError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to serialize message for type {:?}: {}", T::data_type(), self.cause)
    }
}

impl<T: SerDeMessage + Debug> DeserializationError<T> {
    fn new(msg: &str) -> DeserializationError<T> {
        DeserializationError { cause: SerDeErrorCause::Other(msg.to_owned()), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> SerializationError<T> {
    fn new(msg: &str) -> SerializationError<T> {
        SerializationError { cause: SerDeErrorCause::Other(msg.to_owned()), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> From<CapnpError> for DeserializationError<T> {
    fn from(error: CapnpError) -> DeserializationError<T> {
        DeserializationError { cause: SerDeErrorCause::CapnpError(error), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> From<IoError> for DeserializationError<T> {
    fn from(error: IoError) -> DeserializationError<T> {
        DeserializationError { cause: SerDeErrorCause::IoError(error), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> From<CapnpError> for SerializationError<T> {
    fn from(error: CapnpError) -> SerializationError<T> {
        SerializationError { cause: SerDeErrorCause::CapnpError(error), phantom: PhantomData }
    }
}

impl<T: SerDeMessage + Debug> From<IoError> for SerializationError<T> {
    fn from(error: IoError) -> SerializationError<T> {
        SerializationError { cause: SerDeErrorCause::IoError(error), phantom: PhantomData }
    }
}

#[derive(Debug)]
enum SendingErrorCause {
    SerializationError(SerDeErrorCause),
    IoError(IoError)
}

impl fmt::Display for SendingErrorCause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SendingErrorCause::SerializationError(ref error) => write!(f, "serialization error: {}", error),
            &SendingErrorCause::IoError(ref error) => write!(f, "IO Error: {}", error),
        }
    }
}

#[derive(Debug)]
struct SendingError {
    cause: SendingErrorCause
}

impl Error for SendingError {
    fn description(&self) -> &str {
        "failed to send message"
    }
}

impl fmt::Display for SendingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to send message caused by: {}", self.cause)
    }
}

impl<T: SerDeMessage + Debug> From<SerializationError<T>> for SendingError {
    fn from(error: SerializationError<T>) -> SendingError {
        SendingError { cause: SendingErrorCause::SerializationError(error.cause) }
    }
}

impl From<IoError> for SendingError {
    fn from(error: IoError) -> SendingError {
        SendingError { cause: SendingErrorCause::IoError(error) }
    }
}


#[derive(Clone,Copy,Debug,PartialEq)]
pub enum DataType {
    RawDataPoint,
    MessageHeader
}

impl ToString for DataType {
     fn to_string(&self) -> String {
        match self {
            &DataType::RawDataPoint => "RawDataPoint".to_string(),
            &DataType::MessageHeader => "MessageHeader".to_string(),
        }
     }
}

#[derive(Clone,Copy,Debug,PartialEq)]
pub enum Encoding {
    Capnp,
    Plain
}

impl ToString for Encoding {
     fn to_string(&self) -> String {
         match self {
             &Encoding::Capnp => "capnp".to_string(),
             &Encoding::Plain => "plain".to_string(),
         }
     }
}

pub trait SerDeMessage {
    //type SerializationError = SerializationError<Self>;
    //type DeserializationError = DeserializationError<Self>;

    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>>;
    fn data_type() -> DataType;
    fn version() -> u8 {
        0
    }
    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>>;
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
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>> {
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
                        return Err(SerializationError::from(error));
                    }
                }
            },
            Encoding::Plain => {
                warn!("Plain endocing is not implemented for data types");
                Err(SerializationError::not_implemented(Encoding::Plain))
            }
        }
    }

    // TODO: use 'type' alias
    fn data_type() -> DataType {
        DataType::RawDataPoint
    }

    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>> {
        match encoding {
            Encoding::Capnp => {
                let reader = try!(serialize_packed::new_reader_unbuffered(ArrayInputStream::new(bytes), ReaderOptions::new()));
                let raw_data_point = try!(reader.get_root::<super::raw_data_point_capnp::raw_data_point::Reader>());

                Ok(
                    RawDataPoint {
                        location: match raw_data_point.get_location() {
                            Ok(value) => value.to_string(),
                            Err(error) => {
                                error!("Failed to read message for {:?}: {}", <RawDataPoint as SerDeMessage>::data_type(), error);
                                return Err(DeserializationError::from(error))
                            }
                        },
                        path: match raw_data_point.get_path() {
                            Ok(value) => value.to_string(),
                            Err(error) => {
                                error!("Failed to read message for {:?}: {}", <RawDataPoint as SerDeMessage>::data_type(), error);
                                return Err(DeserializationError::from(error))
                            }
                        },
                        component: "iowait".to_string(),
                        timestamp: UTC::now(),
                        value: DataValue::Float(0.2)
                    }
                )
            },
            Encoding::Plain => {
                warn!("Plain endocing is not implemented for data types");
                Err(DeserializationError::not_implemented(Encoding::Plain))
            }
        }
    }
}

/*
enum RecvMessageType {
    RawDataPoint(RawDataPoint),
    Unknown { data_type: String }
}

struct RecvMessage {
    message: RecvMessageType,
    topic: String
}
*/

#[derive(Debug)]
pub struct MessageHeader {
    data_type: DataType,
    topic: String,
    version: u8,
    encoding: Encoding,
}

impl SerDeMessage for MessageHeader {
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>> {
        let encoding = self.encoding.to_string();
        let data_type = self.data_type.to_string();
        Ok(format!("{}/{}\n{}\n{}\n\n", data_type, self.topic, self.version, encoding).into_bytes())
    }

    fn data_type() -> DataType {
        DataType::MessageHeader
    }

    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>> {
        // TODO: don't ignore encoding
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
                            // TODO: move this to error type!
                            _ => return Err(DeserializationError::new(&*format!("unknown data type: {}", string)))
                        },
                        Err(utf8_error) => return Err(DeserializationError::new(&*format!("error decoding data type string: {}", utf8_error)))
                    },
                    None => return Err(DeserializationError::new("no data type found in message header"))
                };
                topic = match splits.next() {
                    Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                        Ok(string) => string,
                        Err(utf8_error) => return Err(DeserializationError::new(&*format!("error decoding topic string: {}", utf8_error)))
                    },
                    None => return Err(DeserializationError::new("no topic found in message header"))
                };
            },
            None => return Err(DeserializationError::new("no data type/topic part found in message header"))
        };

        let version = match parts.next() {
            Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                Ok(string) => match string.parse::<u8>() {
                    Ok(int) => int,
                    Err(parse_error) => return Err(DeserializationError::new(&*format!("message version is not u8 number: {}", parse_error)))
                },
                Err(utf8_error) => return Err(DeserializationError::new(&*format!("error decoding version string: {}", utf8_error)))
            },
            None => return Err(DeserializationError::new("no version found in message header"))
        };

        let encoding = match parts.next() {
            Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                Ok(string) => match &*string {
                    "capnp" => Encoding::Capnp,
                    _ => return Err(DeserializationError::new(&*format!("unknown encoding: {}", string)))
                },
                Err(utf8_error) => return Err(DeserializationError::new(&*format!("error decoding encoding string: {}", utf8_error)))
            },
            None => return Err(DeserializationError::new("no encoding found in message header"))
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
}

pub trait SendMessage<T: SerDeMessage + Debug> {
    fn send_message(&mut self, topic: String, message: T, encoding: Encoding) -> Result<(), SendingError>;
}

impl<T: SerDeMessage + Debug> SendMessage<T> for Socket {
    fn send_message(&mut self, topic: String, message: T, encoding: Encoding) -> Result<(), SendingError>
        where T: SerDeMessage + Debug {
        let mut data: Vec<u8>;

        let header = MessageHeader {
            data_type: T::data_type(),
            topic: topic,
            version: T::version(),
            encoding: encoding
        };
        data = try!(header.to_bytes(Encoding::Plain));

        let body = try!(message.to_bytes(encoding));

        data.extend(body);

        debug!("Sending message with {:?} on topic {}", T::data_type(), header.topic);
        try!(self.write(&data));
        trace!("Message sent");
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
    pub use std::error::Error;

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

            let bytes = header.to_bytes(Encoding::Plain).unwrap();
            assert_eq!(bytes, "RawDataPoint/hello\n42\ncapnp\n\n".to_string().into_bytes());
        }

        describe! deserialization {
            it "should deserialize correctly formated message header" {
                let bytes = "RawDataPoint/hello\n42\ncapnp\n\n".to_string().into_bytes();

                let header = MessageHeader::from_bytes(&bytes, Encoding::Plain).unwrap();
                assert_eq!(header.data_type, DataType::RawDataPoint);
                assert_eq!(header.topic, "hello".to_string());
                assert_eq!(header.version, 42);
                assert_eq!(header.encoding, Encoding::Capnp);
            }

            it "should deserialize message with empty topic" {
                let bytes = "RawDataPoint/\n42\ncapnp\n\n".to_string().into_bytes();

                let header = MessageHeader::from_bytes(&bytes, Encoding::Plain).unwrap();
                assert_eq!(header.data_type, DataType::RawDataPoint);
                assert_eq!(header.topic, "".to_string());
                assert_eq!(header.version, 42);
                assert_eq!(header.encoding, Encoding::Capnp);
            }

            it "should deserialize message with extra sections" {
                let bytes = "RawDataPoint/\n42\ncapnp\nblah\n\n".to_string().into_bytes();

                let header = MessageHeader::from_bytes(&bytes, Encoding::Plain).unwrap();
                assert_eq!(header.data_type, DataType::RawDataPoint);
                assert_eq!(header.topic, "".to_string());
                assert_eq!(header.version, 42);
                assert_eq!(header.encoding, Encoding::Capnp);
            }

            describe! error_handling {
                it "should provide error when not enought header sections are provided" {
                    {
                        let bytes = "RawDataPoint/hello\n42\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        // TODO: how do I test Dispaly output?
                        assert_eq!(err.description(), "no encoding found in message header");
                    }
                    {
                        let bytes = "RawDataPoint/hello\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        assert_eq!(err.description(), "no version found in message header");
                    }
                    {
                        let bytes = "RawDataPoint\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        assert_eq!(err.description(), "no topic found in message header");
                    }
                    {
                        let bytes = "\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        assert_eq!(err.description(), "no data type/topic part found in message header");
                    }
                }

                it "should provide error when version is not a positive integer" {
                    {
                        let bytes = "RawDataPoint/hello\n-1\ncapnp\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        assert!(err.description().starts_with("message version is not u8 number"));
                    }
                    {
                        let bytes = "RawDataPoint/hello\n300\ncapnp\n\n".to_string().into_bytes();

                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert!(result.is_err());
                        let err = result.unwrap_err();
                        assert!(err.description().starts_with("message version is not u8 number"));
                    }
                }

                it "should provide error when unknown encoding was found in the message" {
                    let bytes = "RawDataPoint/hello\n42\ncapn planet\n\n".to_string().into_bytes();

                    let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                    assert!(result.is_err());
                    let err = result.unwrap_err();
                    assert!(err.description().starts_with("unknown encoding: capn planet"));
                }
            }
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

