#![feature(io)]
#![feature(plugin)]
#![plugin(stainless)]
extern crate clap;
use clap::{Arg, App};

#[macro_use] extern crate log;
extern crate env_logger;
extern crate nanomsg;
extern crate chrono;

extern crate capnp;
extern crate capnpc;

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!("./schema/raw_data_point_capnp.rs");
}

mod collector;
mod messaging;

fn main() {
	env_logger::init().unwrap();

	let _ = App::new("MyApp")
		.version("0.0")
		.author("JP")
		.about("Does awesome things")
		.arg(Arg::new("output")
			.help("Name")
			.index(1)
		).get_matches();

    let collector_thread = collector::CollectorThread::spawn("ipc:///tmp/rdms_data_store.ipc");

    let mut collector = collector_thread.new_collector();

    collector.collect("myserver", "os/cpu/usage", "user", collector::DataValue::Float(0.4));
}

