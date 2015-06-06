use super::super::serde::*;

#[derive(Debug)]
pub struct MessageHeader {
    pub data_type: DataType,
    pub topic: String,
    pub version: u8,
    pub encoding: Encoding,
}

impl SerDeMessage for MessageHeader {
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>> {
        match encoding {
            Encoding::Plain => {
                let encoding = self.encoding.to_string();
                let data_type = self.data_type.to_string();
                Ok(format!("{}/{}\n{}\n{}\n\n", data_type, self.topic, self.version, encoding).into_bytes())
            },
            _ => unimplemented!()
        }
    }

    fn data_type() -> DataType {
        DataType::MessageHeader
    }

    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>> {
        match encoding {
            Encoding::Plain => {
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
                                    _ => return Err(DeserializationError::new(SerDeErrorKind::UnknownDataType(string)))
                                },
                                Err(utf8_error) => return Err(DeserializationError::new(SerDeErrorKind::FromUtf8Error("data type", utf8_error)))
                            },
                            None => return Err(DeserializationError::new(SerDeErrorKind::MissingField("data type")))
                        };
                        topic = match splits.next() {
                            Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                                Ok(string) => string,
                                Err(utf8_error) => return Err(DeserializationError::new(SerDeErrorKind::FromUtf8Error("topic", utf8_error)))
                            },
                            None => return Err(DeserializationError::new(SerDeErrorKind::MissingField("topic")))
                        };
                    },
                    None => return Err(DeserializationError::new(SerDeErrorKind::MissingField("data type/topic")))
                };

                let version = match parts.next() {
                    Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                        Ok(string) => match string.parse::<u8>() {
                            Ok(int) => int,
                            Err(parse_error) => return Err(DeserializationError::new(SerDeErrorKind::InvalidVersionNumber(parse_error)))
                        },
                        Err(utf8_error) => return Err(DeserializationError::new(SerDeErrorKind::FromUtf8Error("version", utf8_error)))
                    },
                    None => return Err(DeserializationError::new(SerDeErrorKind::MissingField("version")))
                };

                let encoding = match parts.next() {
                    Some(bytes) => match String::from_utf8(Vec::from(bytes)) {
                        Ok(string) => match &*string {
                            "capnp" => Encoding::Capnp,
                            _ => return Err(DeserializationError::new(SerDeErrorKind::UnknownEncoding(string)))
                        },
                        Err(utf8_error) => return Err(DeserializationError::new(SerDeErrorKind::FromUtf8Error("encoding", utf8_error)))
                    },
                    None => return Err(DeserializationError::new(SerDeErrorKind::MissingField("encoding")))
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
            },
            _ => {
                unimplemented!()
            }
        }
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use super::super::super::serde::*;
    pub use nanomsg::{Socket, Protocol};
    pub use std::thread;
    pub use std::io::Read;
    pub use chrono::*;
    pub use std::error::Error;
    pub use std::fmt::Write;
    pub use std::fmt::Debug;

    macro_rules! assert_error_display_message {
        ($result:expr, $msg:expr) => {{
            assert!($result.is_err());
            let err = $result.unwrap_err();
            let mut message = String::new();
            write!(&mut message, "{}", err).unwrap();
            assert_eq!(message, $msg);
        }}
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
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: no encoding found in message header");
                    }
                    {
                        let bytes = "RawDataPoint/hello\n\n".to_string().into_bytes();
                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: no version found in message header");
                    }
                    {
                        let bytes = "RawDataPoint\n\n".to_string().into_bytes();
                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: no topic found in message header");
                    }
                    {
                        let bytes = "\n\n".to_string().into_bytes();
                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: no data type/topic found in message header");
                    }
                }

                it "should provide error when version is not a positive integer" {
                    {
                        let bytes = "RawDataPoint/hello\n-1\ncapnp\n\n".to_string().into_bytes();
                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: message version is not u8 number: invalid digit found in string");
                    }
                    {
                        let bytes = "RawDataPoint/hello\n300\ncapnp\n\n".to_string().into_bytes();
                        let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                        assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: message version is not u8 number: number too large to fit in target type");
                    }
                }

                it "should provide error when unknown encoding was found in the message" {
                    let bytes = "RawDataPoint/hello\n42\ncapn planet\n\n".to_string().into_bytes();
                    let result = MessageHeader::from_bytes(&bytes, Encoding::Plain);
                    assert_error_display_message!(result, "failed to deserializae message for type MessageHeader: unknown encoding: capn planet");
                }
            }
        }
    }
}

