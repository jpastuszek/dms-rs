extern crate clap;
#[cfg(not(test))]
use clap::{Arg, App};

#[macro_use] extern crate log;
extern crate env_logger;
extern crate nanomsg;
extern crate chrono;

extern crate capnp;
extern crate capnpc;

extern crate threadpool;

// this needs to be in root module, see: https://github.com/dwrensha/capnproto-rust/issues/16
#[allow(dead_code)]
mod raw_data_point_capnp {
    include!(concat!(env!("OUT_DIR"), "/messaging/schema/raw_data_point_capnp.rs"));
}

mod messaging;
mod collector;
mod scheduler;

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

    let mut collector = collector_thread.new_collector();

    collector.collect("myserver", "os/cpu/usage", "user", collector::DataValue::Float(0.4));
}

#[cfg(test)]
mod test {
    use super::*;
    use messaging::*;
    use nanomsg::{Socket, Protocol};
    use std::io::Read;
    use chrono::{DateTime, UTC, Duration};
    use std::thread::sleep_ms;

    use collector::{CollectorThread, Collector};
    use scheduler::{TimeSource, Scheduler};
    use threadpool::ThreadPool;

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

    #[test]
    fn scheduling_data_collection() {
        let mut pull = Socket::new(Protocol::Pull).unwrap();
        let mut _endpoint = pull.bind("ipc:///tmp/test-collector.ipc").unwrap();
        {
            let collector_thread = CollectorThread::spawn("ipc:///tmp/test-collector.ipc");
            let probe_thread_pool = ThreadPool::new(10);
            let mut scheduler: Scheduler<(&CollectorThread, &ThreadPool), (), (), _> = Scheduler::new(Duration::milliseconds(500), FakeTimeSource::new());

            scheduler.after(Duration::milliseconds(1000), Box::new(|probe| {
                let &mut (ref collector_thread, ref probe_thread_pool) = probe;
                let mut collector = collector_thread.new_collector();
                probe_thread_pool.execute(move || {
                    collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
                    collector.collect("foobar", "os/cpu/sys", "user", DataValue::Float(0.4));
                });
                Ok(())
            }));

            let out = scheduler.run(&mut (&collector_thread, &probe_thread_pool));
            assert_eq!(out, Ok(vec![Ok(())]));

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

