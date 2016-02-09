extern crate clap;
#[cfg(not(test))]
use clap::{Arg, App};

#[macro_use] extern crate log;
//TODO: use flexi_logger
extern crate env_logger;
extern crate nanomsg;
extern crate chrono;
extern crate time;
extern crate carboxyl;

extern crate capnp;
extern crate capnpc;

extern crate token_scheduler;

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!(concat!(env!("OUT_DIR"), "/messaging/schema/raw_data_point_capnp.rs"));
}

mod messaging;
mod collector;
mod producer;

#[cfg(not(test))]
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

/*
#[cfg(test)]
mod test {
    use messaging::*;
    use nanomsg::{Socket, Protocol};
    use std::io::Read;
    use chrono::{DateTime, UTC, Duration};

    use collector::CollectorThread;
    use scheduler::{TimeSource, Scheduler};

    use probe::Probe;
    use probe::ProbeModule;
    use probe::ProbeRunMode;
    use probe::ProbeRunner;
    use collector::Collector;

    pub struct FakeTimeSource {
        now: DateTime<UTC>
    }

    impl FakeTimeSource {
        fn new() -> FakeTimeSource {
            FakeTimeSource {
                now: UTC::now()
            }
        }
    }

    impl TimeSource for FakeTimeSource {
        fn now(&self) -> DateTime<UTC> {
            self.now
        }

        fn wait(&mut self, duration: Duration) {
            self.now = self.now + duration;
        }
    }

    struct MockProbe {
        collector: Collector
    }
    //unsafe impl Send for MockProbe {}

    impl Probe for MockProbe {
        fn run(&mut self) -> Result<(), String> {
            self.collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
            self.collector.collect("foobar", "os/cpu/sys", "user", DataValue::Float(0.4));
            Ok(())
        }
    }

    #[allow(dead_code)]
    struct MockProbeModule {
        test: u8
    }
    impl ProbeModule for MockProbeModule {
        type P = MockProbe;

        fn run_mode() -> ProbeRunMode {
            ProbeRunMode::Thread
        }

        fn probe(&self, collector: Collector) -> Box<Self::P> {
            Box::new(MockProbe { collector: collector })
        }
    }

    #[test]
    fn scheduling_data_collection_with_probe_runner() {
        let mut pull = Socket::new(Protocol::Pull).unwrap();
        let mut _endpoint = pull.bind("ipc:///tmp/test-collector.ipc").unwrap();
        {
            let collector_thread = CollectorThread::spawn("ipc:///tmp/test-collector.ipc");

            let probe_module = MockProbeModule {test: 0};

            let mut scheduler: Scheduler<ProbeRunner, (), (), _> = Scheduler::new(Duration::milliseconds(500), FakeTimeSource::new());

            scheduler.after(Duration::milliseconds(1000), Box::new(|probe_runner| {
                let collector = collector_thread.new_collector();
                let probe: Box<MockProbe> = probe_module.probe(collector);

                probe_runner.push(probe, MockProbeModule::run_mode());

                Ok(())
            }));

            let mut probe_runner = ProbeRunner::new(10);
            let out = scheduler.run(&mut probe_runner);
            assert_eq!(out, Ok(vec![Ok(())]));

            probe_runner.run();

            let mut msg = Vec::new();
            pull.read_to_end(&mut msg).unwrap();
            let msg_string = String::from_utf8_lossy(&msg);
            assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
            assert!(msg_string.contains("myserver"));

            let mut msg = Vec::new();
            pull.read_to_end(&mut msg).unwrap();
            let msg_string = String::from_utf8_lossy(&msg);
            assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
            assert!(msg_string.contains("foobar"));
        }
    }
}
*/

