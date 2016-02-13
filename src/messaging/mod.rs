use std::marker::PhantomData;
use std::fmt::Debug;
use std::fmt::Display;
use std::any::Any;
use std::error::Error;
use std::io::Error as IoError;
use std::fmt;
use nanomsg::Socket;
use std::io::Write;

pub use self::serde::*;
pub use self::data_types::*;

pub mod serde;
pub mod data_types;

#[derive(Debug)]
enum MessagingErrorKind {
    SerializationError(DataType, SerDeErrorKind),
    IoError(IoError)
}

impl fmt::Display for MessagingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &MessagingErrorKind::SerializationError(ref data_type, ref error) => write!(f, "serialization error for {:?}: {}", data_type, error),
            &MessagingErrorKind::IoError(ref error) => write!(f, "IO Error: {}", error),
        }
    }
}

trait MessagingDirection: Debug + Any {
    fn direction_name() -> &'static str;
}

#[derive(Debug)]
struct SendingDirection;
#[derive(Debug)]
#[allow(dead_code)]
struct ReceivingDirection;

impl MessagingDirection for SendingDirection {
    fn direction_name() -> &'static str {
        "send"
    }
}

impl MessagingDirection for ReceivingDirection {
    fn direction_name() -> &'static str {
        "receive"
    }
}

#[derive(Debug)]
struct MessagingError<D> where D: MessagingDirection {
    kind: MessagingErrorKind,
    phantom: PhantomData<D>
}

impl<D> Error for MessagingError<D> where D: MessagingDirection {
    fn description(&self) -> &str {
        "messaging error"
    }
}

impl<D> fmt::Display for MessagingError<D> where D: MessagingDirection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to {} message caused by: {}", D::direction_name(), self.kind)
    }
}

impl<D> MessagingError<D> where D: MessagingDirection {
    fn new(kind: MessagingErrorKind) -> MessagingError<D> {
        MessagingError { kind: kind, phantom: PhantomData }
    }
}

impl<T, D> From<SerializationError<T>> for MessagingError<D> where D: MessagingDirection, T: SerDeMessage {
    fn from(error: SerializationError<T>) -> MessagingError<D> {
        MessagingError::new(MessagingErrorKind::SerializationError(error.data_type, error.kind))
    }
}

impl<D> From<IoError> for MessagingError<D> where D: MessagingDirection {
    fn from(error: IoError) -> MessagingError<D> {
        MessagingError::new(MessagingErrorKind::IoError(error))
    }
}

pub type SendingError = MessagingError<SendingDirection>;
pub type ReceivingError = MessagingError<ReceivingDirection>;

pub trait SendMessage<T> where T: SerDeMessage {
        fn send_message<S>(&mut self, topic: S, message: T, encoding: Encoding) -> Result<(), SendingError> where S: Into<String>;
}

impl<T> SendMessage<T> for Socket where T: SerDeMessage {
    fn send_message<S>(&mut self, topic: S, message: T, encoding: Encoding) -> Result<(), SendingError> where S: Into<String>, T: Debug {

        let topic: String = topic.into();
        trace!("Sending message on topic '{}': {:?}", topic, message);

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

        try!(self.write(&data));
        trace!("Message sent");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use nanomsg::{Socket, Protocol};
    pub use std::thread;
    pub use std::io::Read;
    pub use chrono::*;
    pub use std::error::Error;
    pub use std::fmt::Write;
    pub use std::fmt::Debug;

    mod nanomsg_socket_extensions {
        pub use super::*;
        mod send_message {
            pub use super::*;

            #[test]
            fn should_allow_sending_message_with_header_and_capnp_serialized_body() {
                let mut pull = Socket::new(Protocol::Pull).unwrap();
                let mut _endpoint = pull.bind("ipc:///tmp/test.ipc").unwrap();

                let thread = thread::spawn(move || {
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

                    socket.send_message("hello", message, Encoding::Capnp).unwrap();
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
                thread.join().unwrap();
            }
        }
    }
}

