#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate time;
extern crate chrono; // ?
extern crate nanomsg;
extern crate url;
extern crate chan;
extern crate chan_signal;

extern crate capnp;
extern crate capnpc;

extern crate token_scheduler;

use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver};
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

use program::Signal;
use sender::Sender;

fn dms_agent(signals: &Receiver<Signal>, processor_url: &Url) -> Result<(), (String, i32)> {
    //TODO: don't panic on wrong processor address + shutdown correctly
    let sender = Sender::start(processor_url.to_owned()).unwrap();

    let collector = sender.collector();
    let (producer_signal, producer_signals) = channel();
    let producer = producer::spawn(collector, producer_signals);

    loop {
        match signals.recv().expect("signal listener died") {
            signal @ Signal::Shutdown => {
                producer_signal.send(signal).expect("producer thread died");
                //TODO: timeout?
                producer.join().unwrap();

                //TODO: gets stuck on sending
                sender.stop();

                info!("Exiting cleanly");
                return Ok(());
            }
            signal @ Signal::Reload => {
                producer_signal.send(signal).expect("producer thread died");
            }
        }
    }
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

    let signals = program::init(Some(args.value_of("log-spec").unwrap_or("info")));

    let processor_url = value_t!(args, "processor-url", Url).unwrap_or_else(|err|
        match err.kind {
            clap::ErrorKind::ArgumentNotFound => FromStr::from_str("ipc:///tmp/rdms_data_store.ipc").unwrap(),
            _ => err.exit()
        }
    );

    dms_agent(&signals, &processor_url).unwrap_or_else(|(err, code)| program::exit_with_error(err, code));
}
