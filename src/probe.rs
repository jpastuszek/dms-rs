use collector::Collector;
use threadpool::ThreadPool;

#[allow(dead_code)]
pub enum ProbeRunMode {
    Inline,
    Thread,
    //Process
}

pub trait ProbeModule {
    type P: Probe;

    fn run_mode() -> ProbeRunMode;
    fn probe(&self, collector: Collector) -> Box<Self::P>;
}

pub trait Probe: Send {
    fn run(self) -> Result<(), String>;
}

pub struct ProbeRunner {
    thread_pool: ThreadPool
}

impl ProbeRunner {
    pub fn new(threads: u16) -> ProbeRunner {
        ProbeRunner {
            thread_pool: ThreadPool::new(threads as usize)
        }
    }

    pub fn run<P>(&self, probe: Box<P>, probe_run_mode: ProbeRunMode) where P: Probe + 'static {
        match probe_run_mode {
            ProbeRunMode::Inline => {
                probe.run();
            },
            ProbeRunMode::Thread => {
                self.thread_pool.execute(move || {
                    probe.run();
                });
            }
        }
    }

    //TODO: provide a way to collect run errors
}


