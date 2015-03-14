#![feature(old_io)]
#![feature(rustc_private)]
extern crate clap;
//extern crate serialize;
use clap::{Arg, App};

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate msgpack;
extern crate nanomsg;
extern crate chrono;
extern crate serialize;
//extern crate "rustc-serialize" as serialize;
use serialize::{Encodable, Encoder, Decodable, Decoder};

use nanomsg::{Socket, NanoResult, Protocol};

use chrono::*;
use std::ops::*;
use std::old_io::*;

extern crate core;
use core::num::Float;

//#[derive(RustcEncodable,RustcDecodable,PartialEq,Debug)]
//#[derive(Encodable,Decodable,PartialEq,Debug)]
#[derive(Debug)]
struct RawDataPoint<V> {
	location: String,
	path: String,
	component: String,
	time_stamp: DateTime<UTC>,
	value: V,
}

impl<V: Encodable> Encodable for RawDataPoint<V> {
	fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
		s.emit_map(5, |s| {
			try!(s.emit_map_elt_key(0, |s| "location".encode(s)));
			try!(s.emit_map_elt_val(0, |s| self.location.encode(s)));

			try!(s.emit_map_elt_key(1, |s| "path".encode(s)));
			try!(s.emit_map_elt_val(1, |s| self.path.encode(s)));

			try!(s.emit_map_elt_key(2, |s| "component".encode(s)));
			try!(s.emit_map_elt_val(2, |s| self.component.encode(s)));

			let time_stamp = (self.time_stamp.timestamp() as f64) + (self.time_stamp.nanosecond() as f64 / 1_000_000_000f64);
			try!(s.emit_map_elt_key(3, |s| "time_stamp".encode(s)));
			try!(s.emit_map_elt_val(3, |s| time_stamp.encode(s)));

			try!(s.emit_map_elt_key(4, |s| "value".encode(s)));
			try!(s.emit_map_elt_val(4, |s| self.value.encode(s)));

			Ok(())
		})
	}
}

impl Decodable for RawDataPoint<i32> {
  fn decode<D: Decoder>(d: &mut D) -> Result<RawDataPoint<i32>, D::Error> {
		d.read_map(|d, len| {
			let mut out = RawDataPoint {
		  	location: "lll".to_string(), 
		  	path: "ppp".to_string(), 
		  	component: "cccc".to_string(), 
		  	time_stamp: UTC::now(),
		  	value: 666
			};

			for i in 0..len {
				let key: String = try!(d.read_map_elt_key(i, |d| Decodable::decode(d)));
				trace!("Found key: {}", key);

				if key == "location".to_string() {
					out.location = try!(d.read_map_elt_val(i, |d| Decodable::decode(d)));
				} else if key == "path".to_string() {
					out.path = try!(d.read_map_elt_val(i, |d| Decodable::decode(d)));
				} else if key == "component".to_string() {
					out.component = try!(d.read_map_elt_val(i, |d| Decodable::decode(d)));
				} else if key == "time_stamp".to_string() {
					let ts: f64 = try!(d.read_map_elt_val(i, |d| Decodable::decode(d)));

					out.time_stamp = DateTime::from_utc(NaiveDateTime::from_timestamp(
						ts.trunc() as i64,
						(ts.fract() * 1_000_000_000f64) as u32
					), UTC)
				} else if key == "value".to_string() {
					out.value = try!(d.read_map_elt_val(i, |d| Decodable::decode(d)));
				} else {
					// Note: we cannot continue decoding since we don't know what value type we can expect!
					warn!("extra data with key: '{}' while decoding RawDataPoint", key);
					return Ok(out)
				}
			}
			Ok(out)
		})
  }
}

trait ToMsgPack {
	fn to_msgpack(&self) -> IoResult<Vec<u8>>;
}

impl<V: Encodable> ToMsgPack for RawDataPoint<V> {
	fn to_msgpack(&self) -> IoResult<Vec<u8>> {
		//data.insert("time_stamp", self.time_stamp);
	  msgpack::Encoder::to_msgpack(&self)
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

  let data1 = RawDataPoint {
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

  match data1.to_msgpack() {
  		Ok(data2) => {
				//let dec: RawDataPoint<i32> = msgpack::from_msgpack(&data2).ok().unwrap();
				println!("Encoded: {:?}", data2);
  		}
  		Err(error) => {
  			error!("Failed to encode data: {}", error)
  		}
  }

  match data2.to_msgpack() {
  		Ok(data2) => {
				println!("Encoded: {:?}", data2);
  			let r: Result<RawDataPoint<i32>, _> = msgpack::from_msgpack(&data2);
				match r {
					Ok(dec) => {
						println!("Decoded: {:?}", dec);
					}
					Err(error) => {
		  			error!("Failed to decode data: {}", error)
					}
				}
  		}
  		Err(error) => {
  			error!("Failed to encode data: {}", error)
  		}
  }

 // let mut socket = Socket::new(Protocol::Pull).unwrap();
	//let mut endpoint = socket.bind("tcp://*:1112").unwrap();

 // let msg = socket.read_to_string().unwrap();
  // /usr/local/Cellar/nanomsg/0.5-beta/bin/nanocat --verbose --connect-local 1112 --send-timeout 2 --data hello --push
//	println!("We got a message: {}", &*msg);
}

/*
2.2.0 :008 > MessagePack.load [132, 168, 108, 111, 99, 97, 116, 105, 111, 110, 166, 109, 121, 104, 111, 115, 116, 164, 112, 97, 116, 104, 173, 115, 121, 115, 47, 99, 112, 117, 47, 117, 115, 97, 103, 101, 169, 99, 111, 109, 112, 111, 110, 101, 110, 116, 164, 117, 115, 101, 114, 165, 118, 97, 108, 117, 101, 208, 1].pack('c*')
 => {"location"=>"myhost", "path"=>"sys/cpu/usage", "component"=>"user", "value"=>1}
*/
