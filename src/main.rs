extern crate clap;
use clap::{Arg, App};

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate msgpack;
extern crate nanomsg;

use nanomsg::{Socket, NanoResult, Protocol};

fn main() {
	env_logger::init().unwrap();

	let matches = App::new("MyApp")
		.version("0.0")
		.author("JP")
		.about("Does awesome things")
		.arg(Arg::new("output")
			.help("Name")
			.index(1)
		).get_matches();

	println!("Hello, world! {:?}", matches.value_of("output").unwrap_or(&"nothing".to_string()));

  let arr = vec!["str1".to_string(), "str2".to_string()];
  let str = msgpack::Encoder::to_msgpack(&arr).ok().unwrap();
  println!("Encoded: {:?}", str);

  let mut socket = Socket::new(Protocol::Pull).unwrap();
	let mut endpoint = socket.bind("tcp://*:1112").unwrap();

  let msg = socket.read_to_string().unwrap();
  // /usr/local/Cellar/nanomsg/0.5-beta/bin/nanocat --verbose --connect-local 1112 --send-timeout 2 --data hello --push
	println!("We got a message: {}", &*msg);
}
