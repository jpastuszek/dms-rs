use nanomsg::Socket;
use capnp::serialize_packed;
use capnp::{MallocMessageBuilder};
use capnp::io::OutputStream;
use std::io::Error;

pub struct MessageHeader {
    data_type: String,
    topic: String,
    version: i8,
    encoding: String,
}

impl MessageHeader {
    fn to_bytes(& self) -> Vec<u8> {
        format!("{}/{}\n{}\n{}\n\n", self.data_type, self.topic, self.version, self.encoding).into_bytes()
    }
}

pub trait SendMessage {
    fn send_message(&mut self, MessageHeader, &mut MallocMessageBuilder) -> Result<(), Error>;
}

impl SendMessage for Socket {
    fn send_message(&mut self, header: MessageHeader, message: &mut MallocMessageBuilder) -> Result<(), Error> {
        let mut data: Vec<u8> = header.to_bytes();
        match serialize_packed::write_packed_message_unbuffered(&mut data, message) {
            Ok(_) => trace!("Message serialized"),
            Err(error) => {
                error!("Failed to serialize message for {}: {}", header.data_type, error);
                return Err(error);
            }
        }

        debug!("Sending message with {} on topic {}", header.data_type, header.topic);
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
        before_each {
            let mut pull = Socket::new(Protocol::Pull).unwrap();
            let _ = pull.bind("ipc:///tmp/test.ipc").unwrap();
        }

        it "should allow sending message with header and capnp serialized body" {
            let _ = thread::scoped(move || {
                let mut socket = Socket::new(Protocol::Push).unwrap();
                let _ = socket.connect("ipc:///tmp/test.ipc").unwrap();

                let header = MessageHeader {
                    data_type: "TestData".to_string(),
                    topic: "hello world".to_string(),
                    version: 1,
                    encoding: "capnp".to_string()
                };

                let mut message = MallocMessageBuilder::new_default();
                {
                    let timestamp = UTC::now();
                    let mut date_time_builder = message.init_root::<raw_data_point_capnp::date_time::Builder>();
                    date_time_builder.set_unix_timestamp(timestamp.timestamp());
                    date_time_builder.set_nanosecond(timestamp.nanosecond());
                }

                socket.send_message(header, &mut message).unwrap();
            });

            let mut msg = Vec::new();
            match pull.read_to_end(&mut msg) {
                Ok(_) => println!("{:?}", msg),
                Err(error) => panic!("got error: {}", error)
            }
        }
    }
}

