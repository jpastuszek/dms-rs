#![feature(old_io)]
extern crate clap;
//extern crate serialize;
use clap::{Arg, App};

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate nanomsg;
extern crate chrono;

//use nanomsg::{Socket, NanoResult, Protocol};

use chrono::*;
use std::ops::*;
use std::old_io::*;

extern crate capnp;
extern crate capnpc;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};

#[allow(dead_code)]
mod raw_data_point_capnp {
    include!("./schema/raw_data_point_capnp.rs");
}

#[derive(Debug)]
#[allow(dead_code)]
enum DataValue {
	Integer(i64),
	Float(f64),
	Bool(bool),
	Text(String),
}

struct Collector<'a> {
	timestamp: DateTime<UTC>,
	messages: Vec<&'a MallocMessageBuilder>,
}

impl<'a> Collector<'a> {
	fn new() -> Collector<'a> {
		Collector {
			timestamp: UTC::now(),
			messages: Vec::new(),
		}
	}

	fn collect(&self, location: &str, path: &str, component: &str, value: DataValue) -> () {
		let mut message = MallocMessageBuilder::new_default();
		let mut raw_data_point = message.init_root::<raw_data_point_capnp::raw_data_point::Builder>();

		raw_data_point.set_location(location);
		raw_data_point.set_path(path);
		raw_data_point.set_component(component);

		{
			let mut date_time = raw_data_point.borrow().init_timestamp();
			date_time.set_unix_timestamp(self.timestamp.timestamp());
			date_time.set_nanosecond(self.timestamp.nanosecond());
		}

		{
			let mut _value = raw_data_point.borrow().init_value();
			match value {
				DataValue::Integer(value) => {
					_value.set_integer(value);
				},
				DataValue::Float(value) => {
					_value.set_float(value);
				},
				DataValue::Bool(value) => {
					_value.set_boolean(value);
				},
				DataValue::Text(value) => {
					_value.set_text(&*value);
				},
			}
		}
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

		let mut collector = Collector::new();
		collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.2));
}
