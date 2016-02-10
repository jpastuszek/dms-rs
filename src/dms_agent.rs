#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate time;
extern crate chrono; // ?
extern crate nanomsg;
extern crate carboxyl;

extern crate capnp;
extern crate capnpc;

extern crate token_scheduler;

use clap::{ArgMatches, App, Arg};

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!(concat!(env!("OUT_DIR"), "/messaging/schema/raw_data_point_capnp.rs"));
}

mod program;
mod messaging;
mod collector;
mod producer;

fn dms_agent(args: ArgMatches) -> Result<(), (String, i32)> {
    //TODO: don't panic on wrong collector address + shutdown correctly
    let collector_thread = collector::CollectorThread::spawn(args.value_of("collector-url").unwrap_or("ipc:///tmp/rdms_data_store.ipc"));

    let collector = collector_thread.new_collector();

    producer::start(collector);

    Ok(())
}

fn main() {
    let args = App::new("Distributed Monitoring System Agent")
        .version(crate_version!())
        .author("Jakub Pastuszek <jpastuszek@whatclinic.com>")
        .about("Produces raw measurement data by polling and exposes various interfaces to inject data from external sources")
        .arg(Arg::with_name("log-spec")
             .short("l")
             .long("log-sepc")
             .value_name("LOG_LEVEL_SPEC")
             .help("Logging level specification, e.g: dms_agent=info")
             .takes_value(true))
        .arg(Arg::with_name("collector-url")
             .short("c")
             .long("collector-url")
             .value_name("URL")
             .help("Nanomsg URL to collector [ipc:///tmp/rdms_data_store.ipc]")
             .takes_value(true))
        .get_matches();

    program::init(args.value_of("log-spec"));

    dms_agent(args).unwrap_or_else(|(err, code)| program::exit_with_error(err, code));
}
