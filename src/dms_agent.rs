#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate time;
extern crate chrono; // ?
extern crate nanomsg;
extern crate url;
extern crate carboxyl;

extern crate capnp;
extern crate capnpc;

extern crate token_scheduler;

use std::str::FromStr;
use clap::{App, Arg};
use url::Url;

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!(concat!(env!("OUT_DIR"), "/messaging/schema/raw_data_point_capnp.rs"));
}

mod program;
mod messaging;
mod sender;
mod producer;

use sender::Sender;

fn dms_agent(url: &Url) -> Result<(), (String, i32)> {
    //TODO: don't panic on wrong processor address + shutdown correctly
    let sender = Sender::spawn(url.to_owned()).unwrap();

    let collector = sender.collector();

    producer::start(collector);

    Ok(())
}

//TODO: update capnp
//TODO: pass location arg to raw data points
//TODO: set timestamp on raw data points to current batch
fn main() {
    let args = App::new("Distributed Monitoring System Agent")
        .version(crate_version!())
        .author("Jakub Pastuszek <jpastuszek@whatclinic.com>")
        .about("Produces raw measurement data by polling and exposes various interfaces to inject data from external sources")
        .arg(Arg::with_name("log-spec")
             .short("l")
             .long("log-sepc")
             .value_name("LOG_LEVEL_SPEC")
             .help("Logging level specification, e.g: [info]")
             .takes_value(true))
        .arg(Arg::with_name("processor-url")
             .short("c")
             .long("processor-url")
             .value_name("URL")
             .help("Nanomsg URL to raw data processor [ipc:///tmp/dms_processor.ipc]")
             .takes_value(true))
        .get_matches();

    program::init(Some(args.value_of("log-spec").unwrap_or("info")));

    let url = value_t!(args, "processor-url", Url).unwrap_or_else(|err|
        match err.kind {
            clap::ErrorKind::ArgumentNotFound => FromStr::from_str("ipc:///tmp/rdms_data_store.ipc").unwrap(),
            _ => err.exit()
        }
    );

    dms_agent(&url).unwrap_or_else(|(err, code)| program::exit_with_error(err, code));
}
