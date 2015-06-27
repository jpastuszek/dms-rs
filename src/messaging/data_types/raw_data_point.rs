use std::io::BufReader;
use std::io::Cursor;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder, MessageReader};
use capnp::message::ReaderOptions;
use chrono::{DateTime, UTC, Timelike};

use super::super::serde::*;

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
    pub value: DataValue
}

impl SerDeMessage for RawDataPoint {
    fn to_bytes(&self, encoding: Encoding) -> Result<Vec<u8>, SerializationError<Self>> {
        match encoding {
            Encoding::Capnp => {
                let mut message = MallocMessageBuilder::new_default();
                {
                    let mut raw_data_point_builder = message.init_root::<::raw_data_point_capnp::raw_data_point::Builder>();

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
                            DataValue::Integer(value) => value_builder.set_integer(value),
                            DataValue::Float(value) => value_builder.set_float(value),
                            DataValue::Bool(value) => value_builder.set_boolean(value),
                            DataValue::Text(ref value) => value_builder.set_text(&*value)
                        }
                    }
                }

                let mut data = Vec::new();
                try!(serialize_packed::write_message(&mut data, &mut message));
                Ok(data)
            },
            Encoding::Plain => unimplemented!()
        }
    }

    fn data_type() -> DataType {
        DataType::RawDataPoint
    }

    fn from_bytes(bytes: &Vec<u8>, encoding: Encoding) -> Result<Self, DeserializationError<Self>> {
        match encoding {
            Encoding::Capnp => {
                let mut buf_reader = BufReader::new(Cursor::new(bytes.clone()));
                let reader = try!(serialize_packed::read_message(&mut buf_reader, ReaderOptions::new()));
                let raw_data_point = try!(reader.get_root::<::raw_data_point_capnp::raw_data_point::Reader>());

                Ok(
                    RawDataPoint {
                        location: try!(raw_data_point.get_location()).to_string(),
                        path: try!(raw_data_point.get_path()).to_string(),
                        component: "iowait".to_string(),
                        timestamp: UTC::now(),
                        value: DataValue::Float(0.2)
                    }
                )
            },
            Encoding::Plain => Err(From::from(SerDeErrorKind::EncodingNotImplemented(Encoding::Plain)))
        }
    }
}

