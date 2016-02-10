extern crate clap;
#[macro_use]
extern crate log;
//TODO: use flexi_logger
extern crate env_logger;
extern crate nanomsg;
extern crate chrono;
extern crate time;
extern crate carboxyl;

extern crate capnp;
extern crate capnpc;

extern crate token_scheduler;

use clap::{Arg, App};

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!(concat!(env!("OUT_DIR"), "/messaging/schema/raw_data_point_capnp.rs"));
}

mod messaging;
mod collector;
mod producer;

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

    let collector = collector_thread.new_collector();

    producer::start(collector)
}

