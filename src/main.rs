extern crate clap;
use clap::{Arg, App};

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate msgpack;
extern crate nanomsg;
extern crate chrono;

use nanomsg::{Socket, NanoResult, Protocol};

use std::any::Any;
use chrono::*;
use std::ops::*;

struct RawDataPoint<V> {
	location: String,
	path: String,
	component: String,
	time_stamp: DateTime<UTC>,
	value: V,
}

impl RawDataPoint<i32> {
	fn to_msgpack(&self) -> Vec<u8> {
		let arr = vec![self.location.clone()];
	  msgpack::Encoder::to_msgpack(&arr).ok().unwrap()
	}
}

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

  let data = RawDataPoint {
  	location: "myhost".to_string(), 
  	path: "sys/cpu/usage".to_string(), 
  	component: "user".to_string(), 
  	time_stamp: UTC::now(),
  	value: 0.3
  };

  let data2 = RawDataPoint {
  	location: "myhost".to_string(), 
  	path: "sys/cpu/usage".to_string(), 
  	component: "user".to_string(), 
  	time_stamp: UTC::now(),
  	value: 1
  };

  println!("Encoded: {:?}", data2.to_msgpack());
  let dec: Vec<String> = msgpack::from_msgpack(data2.to_msgpack().as_mut_slice()).ok().unwrap();
  println!("Decode: {}", dec[0]);

 // let mut socket = Socket::new(Protocol::Pull).unwrap();
	//let mut endpoint = socket.bind("tcp://*:1112").unwrap();

 // let msg = socket.read_to_string().unwrap();
  // /usr/local/Cellar/nanomsg/0.5-beta/bin/nanocat --verbose --connect-local 1112 --send-timeout 2 --data hello --push
//	println!("We got a message: {}", &*msg);
}
