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
use std::sync::mpsc::channel;
use chan::Receiver;
use clap::{App, Arg};
use url::Url;
use chan_signal::Signal;

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

fn dms_agent(signals: &Receiver<Signal>, processor_url: &Url) -> Result<(), (String, i32)> {
    //TODO: don't panic on wrong processor address + shutdown correctly
    let sender = Sender::spawn(processor_url.to_owned()).unwrap();

    let collector = sender.collector();
    let (producer_notify, producer_signals) = channel();
    let producer = producer::spawn(collector, producer_signals);

    debug!("Waiting for signals...");
    let signal = signals.recv().expect("chan_signal thread died");
    debug!("Process received signal: {:?}", signal);

    producer_notify.send(signal).expect("producer thread died");
    //NOTE: assuming shutdown on any signal
    //TODO: timeout?
    producer.join().unwrap();

    sender.stop();

    info!("Exiting cleanly");
    Ok(())
}

//TODO: update capnp
//TODO: pass location arg to raw data points
//TODO: set timestamp on raw data points to current batch
fn main() {
    //TODO: move to own function; return channel from std with custom signals (shutdown, reload)
    //NOTE: this has to be called before any thread is spawned
    let signals = chan_signal::notify(&[Signal::INT, Signal::TERM]);

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

    let processor_url = value_t!(args, "processor-url", Url).unwrap_or_else(|err|
        match err.kind {
            clap::ErrorKind::ArgumentNotFound => FromStr::from_str("ipc:///tmp/rdms_data_store.ipc").unwrap(),
            _ => err.exit()
        }
    );

    dms_agent(&signals, &processor_url).unwrap_or_else(|(err, code)| program::exit_with_error(err, code));
}
