use collector::Collector;
use asynchronous::{Deferred, ControlFlow};

#[allow(dead_code)]
pub enum ProbeRunMode {
    Inline,
    Thread,
    //Process
}

pub trait ProbeModule {
    type P: Probe;

    fn run_mode() -> ProbeRunMode;
    fn probe(&self, collector: Collector) -> Self::P;
}

pub trait Probe: Send {
    fn run(self) -> Result<(), String>;
}

pub struct ProbeRunner {
    threaded: Vec<Deferred<(), String>>,
    // Can't do Box<FnOnce> - try wrapping with sized object (InlineDeffered?)with pointer? + lifetime?
    inline: Vec<Box<FnOnce() -> Result<(), String> + Send>>
}

impl ProbeRunner {
    pub fn new(threads: u16) -> ProbeRunner {
        ProbeRunner {
            threaded: vec![],
            inline: vec![]
        }
    }

    pub fn push<P>(&mut self, probe: P, probe_run_mode: ProbeRunMode) where P: Probe + 'static {
        match probe_run_mode {
            ProbeRunMode::Inline => {
                self.inline.push(Box::new(move || {
                    probe.run()
                }));
            },
            ProbeRunMode::Thread => {
                self.threaded.push(Deferred::new(move || {
                    probe.run()
                }));
            }
        }
    }

    pub fn run(self) {
        let mut deferred = vec![];

        let inline = self.inline;
/* see http://stackoverflow.com/questions/30411594/moving-a-boxed-function
        deferred.push(Deferred::new(move || {
            for probe in inline.into_iter() {
                probe();
            }
            Ok(())
        }));
        */

        deferred.extend(self.threaded);
        let promise = Deferred::vec_to_promise(deferred, ControlFlow::Parallel);

        // TODO: handle errors from promises
        promise.sync().unwrap();
    }
}


