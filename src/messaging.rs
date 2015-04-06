use nanomsg::{Socket, NanoResult, Protocol};
use capnp::{MallocMessageBuilder};

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

trait SendMessage {
    fn send_message(&mut self, MessageHeader, MallocMessageBuilder);
}

impl SendMessage for Socket {
    fn send_message(&mut self, header: MessageHeader, data: MallocMessageBuilder) {
        self.write(&header.to_bytes());
    }
}

describe! nanomsg_socket_extension {
    before_each {
        use nanomsg::{Socket, NanoResult, Protocol};
        use capnp::{MallocMessageBuilder};
        use messaging::SendMessage;
        use std::thread;
        use std::thread::JoinGuard;

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

            println!("sending");
            socket.send_message(header, message);
            println!("sent");
        });

        println!("reading");
        match pull.read_to_end() {
            Ok(msg) => println!("{:?}", msg),
            Err(error) => println!("got error: {}", error)
        }
        println!("read");
        thread.join();
    }
}

