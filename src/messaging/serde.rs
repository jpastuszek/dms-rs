use std::marker::PhantomData;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::error::Error;
use std::io::Error as IoError;
use std::string::FromUtf8Error;
use std::num::ParseIntError;
use capnp::Error as CapnpError;

#[derive(Debug)]
pub enum SerDeErrorKind {
    CapnpError(CapnpError),
    IoError(IoError),
    EncodingNotImplemented(Encoding),
    UnknownEncoding(String),
    UnknownDataType(String),
    FromUtf8Error(&'static str, FromUtf8Error),
    MissingField(&'static str),
    InvalidVersionNumber(ParseIntError),
}

impl Display for SerDeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SerDeErrorKind::CapnpError(ref error) => write!(f, "Cap'n Proto Error: {}", error),
            &SerDeErrorKind::IoError(ref error) => write!(f, "IO Error: {}", error),
            &SerDeErrorKind::EncodingNotImplemented(ref enc) => write!(f, "encoding '{}' not implemented", enc.to_string()),
            &SerDeErrorKind::UnknownEncoding(ref enc) => write!(f, "unknown encoding: {}", enc),
            &SerDeErrorKind::UnknownDataType(ref type_name) => write!(f, "unknown data type: {}", type_name),
            &SerDeErrorKind::FromUtf8Error(ref field_name, ref error) => write!(f, "error decoding {} string: {}", field_name, error),
            &SerDeErrorKind::MissingField(ref field_name) => write!(f, "no {} found in message header", field_name),
            &SerDeErrorKind::InvalidVersionNumber(ref error) => write!(f, "message version is not u8 number: {}", error),
        }
    }
}

// TODO:
// * do I need Error trait for DeserializationError<T> - used in tests; used internally as error
// but never exposed directly due to being templated by data type
// * how do I data_type based on the object T itself? (reflection?) - no; there is reflection
// (unstable) but more for Any type support and testing if types equal or not
// * can I deduplicate code for DeserializationError<T>/SerializationError<T>
//  * common trait - I need a solid type for From to work with; I could use default impl - won't
//  work since I cannot implement not mine trait (From) to potentially not mine object T: MyTrait
// * how can I separete encodings for header from encodings for data type
// * should I use from(kind::Enum) or new(kind::Enum) for internal errors? - new sounds better

#[derive(Debug)]
pub struct DeserializationError<T> where T: SerDeMessage {
    pub kind: SerDeErrorKind,
    pub data_type: DataType,
    phantom: PhantomData<T>
}

impl<T> Error for DeserializationError<T> where T: SerDeMessage {
    fn description(&self) -> &str {
        "deserialization error"
    }
}

impl<T> fmt::Display for DeserializationError<T> where T: SerDeMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to deserializae message for type {:?}: {}", T::data_type(), self.kind)
    }
}

impl<T> DeserializationError<T> where T: SerDeMessage {
    pub fn new(kind: SerDeErrorKind) -> DeserializationError<T> {
        DeserializationError { kind: kind, data_type: T::data_type(), phantom: PhantomData }
    }
}

impl<T> From<SerDeErrorKind> for DeserializationError<T> where T: SerDeMessage {
    fn from(kind: SerDeErrorKind) -> DeserializationError<T> {
        DeserializationError::new(kind)
    }
}

#[derive(Debug)]
pub struct SerializationError<T> where T: SerDeMessage {
    pub kind: SerDeErrorKind,
    pub data_type: DataType,
    phantom: PhantomData<T>
}

impl<T> Error for SerializationError<T> where T: SerDeMessage {
    fn description(&self) -> &str {
        "serialization error"
    }
}

impl<T> fmt::Display for SerializationError<T> where T: SerDeMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to serialize message for type {:?}: {}", T::data_type(), self.kind)
    }
}

impl<T> SerializationError<T> where T: SerDeMessage {
    pub fn new(kind: SerDeErrorKind) -> SerializationError<T> {
        SerializationError { kind: kind, data_type: T::data_type(), phantom: PhantomData }
    }
}

impl<T> From<SerDeErrorKind> for SerializationError<T> where T: SerDeMessage {
    fn from(kind: SerDeErrorKind) -> SerializationError<T> {
        SerializationError::new(kind)
    }
}

// TODO: implement marker trait for both DeserializationError and SerializationError thich inherits
// From<SerDeErrorKind> (needs to be implemented for both types) and define this as generic
// impelemntation for types implementing this marker trait
/*
trait SerDeError: From<SerDeErrorKind> { }
impl<T: SerDeMessage > SerDeError for DeserializationError<T> { }
impl<T: SerDeMessage > SerDeError for SerializationError<T> { }
impl<T> From<CapnpError> for T where T: SerDeError {
    fn from(error: CapnpError) -> T {
        T::from(SerDeErrorKind::CapnpError(error))
    }
}
error: type parameter `T` must be used as the type parameter for some local type (e.g. `MyStruct<T>`); only traits defined in the current crate can be implemented for a type parameter
*/

impl<T> From<IoError> for DeserializationError<T> where T: SerDeMessage {
    fn from(error: IoError) -> DeserializationError<T> {
        From::from(SerDeErrorKind::IoError(error))
    }
}

impl<T> From<IoError> for SerializationError<T> where T: SerDeMessage {
    fn from(error: IoError) -> SerializationError<T> {
        From::from(SerDeErrorKind::IoError(error))
    }
}

impl<T> From<CapnpError> for DeserializationError<T> where T: SerDeMessage {
    fn from(error: CapnpError) -> DeserializationError<T> {
        From::from(SerDeErrorKind::CapnpError(error))
    }
}

impl<T> From<CapnpError> for SerializationError<T> where T: SerDeMessage {
    fn from(error: CapnpError) -> SerializationError<T> {
        From::from(SerDeErrorKind::CapnpError(error))
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

pub trait SerDeMessage: Debug {
    //type SerializationError = SerializationError<Self>;
    //type DeserializationError = DeserializationError<Self>;

    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>>;
    fn data_type() -> DataType;
    fn version() -> u8 {
        0
    }
    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>>;
}

