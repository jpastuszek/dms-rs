use nanomsg::{Socket, Protocol};
use capnp::serialize_packed;
use capnp::{MallocMessageBuilder};
use capnp::io::OutputStream;
//use std::io::Write;
use std::io::Result;

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

struct MsgBuf {
    out: Vec<u8>
}

impl OutputStream for MsgBuf {
    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.out.push_all(buf);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> { Ok(()) }
}

trait SendMessage {
    fn send_message(&mut self, MessageHeader, &mut MallocMessageBuilder);
}

impl SendMessage for Socket {
    fn send_message(&mut self, header: MessageHeader, message: &mut MallocMessageBuilder) {
        let mut data: Vec<u8> = header.to_bytes();
        let mut buf = MsgBuf { out: Vec::new() };

        serialize_packed::write_packed_message_unbuffered(&mut buf, message).ok().unwrap();

        println!("write");
        data.extend(buf.out);
        self.write(&data);
        println!("write done");
    }
}

describe! nanomsg_socket_extensions {
    before_each {
        use nanomsg::{Socket, Protocol};
        use capnp::{MessageBuilder, MallocMessageBuilder};
        use messaging::SendMessage;
        use std::*;
        use std::thread::JoinGuard;
        use std::io::Read;
        use chrono::*;

        #[allow(dead_code)]
        mod raw_data_point_capnp {
            include!("./schema/raw_data_point_capnp.rs");
        }

        let mut pull = Socket::new(Protocol::Pull).unwrap();
        let pull_endpoint = pull.bind("ipc:///tmp/test.ipc").unwrap();
    }

    it "should allow sendign message" {
        let thread = thread::scoped(move || {
            let mut socket = Socket::new(Protocol::Push).unwrap();
            let endpoint = socket.connect("ipc:///tmp/test.ipc").unwrap();

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

            socket.send_message(header, &mut message);
        });

        let mut msg = Vec::new();
        match pull.read_to_end(&mut msg) {
            Ok(_) => println!("{:?}", msg),
            Err(error) => panic!("got error: {}", error)
        }
    }
}

