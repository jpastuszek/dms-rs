use std::marker::PhantomData;
use std::marker::MarkerTrait;
use std::str::FromStr;
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

pub trait SerDeDirection: MarkerTrait + Debug {
    fn direction_name() -> &'static str;
}

#[derive(Debug)]
struct SerializationDirection;
#[derive(Debug)]
struct DeserializationDirection;

impl SerDeDirection for SerializationDirection {
    fn direction_name() -> &'static str {
        "serialize"
    }
}

impl SerDeDirection for DeserializationDirection {
    fn direction_name() -> &'static str {
        "deserializae"
    }
}

#[derive(Debug)]
pub struct SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    pub kind: SerDeErrorKind,
    pub data_type: DataType,
    phantom_t: PhantomData<T>,
    phantom_d: PhantomData<D>
}

impl<T, D> Error for SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    fn description(&self) -> &str {
        "serialization/deserialization error"
    }
}

impl<T, D> fmt::Display for SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to {} message for type {:?}: {}", D::direction_name(), T::data_type(), self.kind)
    }
}

impl<T, D> SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    pub fn new(kind: SerDeErrorKind) -> SerDeError<T, D> {
        SerDeError { kind: kind, data_type: T::data_type(), phantom_t: PhantomData, phantom_d: PhantomData }
    }
}

impl<T, D> From<SerDeErrorKind> for SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    fn from(kind: SerDeErrorKind) -> SerDeError<T, D> {
        SerDeError::new(kind)
    }
}

impl<T, D> From<IoError> for SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    fn from(error: IoError) -> SerDeError<T, D> {
        From::from(SerDeErrorKind::IoError(error))
    }
}

impl<T, D> From<CapnpError> for SerDeError<T, D> where T: SerDeMessage, D: SerDeDirection {
    fn from(error: CapnpError) -> SerDeError<T, D> {
        From::from(SerDeErrorKind::CapnpError(error))
    }
}

pub type SerializationError<T> = SerDeError<T, SerializationDirection>;
pub type DeserializationError<T> = SerDeError<T, DeserializationDirection>;

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

impl<'a> FromStr for DataType {
    type Err = &'a str;
    fn from_str<'b>(string: &'b str) -> Result<Self, &'b str> {
        match string {
            "RawDataPoint" => Ok(DataType::RawDataPoint),
            "MessageHeader" => Ok(DataType::MessageHeader),
            _ => Err(string)
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
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>>;
    fn data_type() -> DataType;
    fn version() -> u8 {
        0
    }
    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>>;
}

