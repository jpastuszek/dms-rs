use std::slice::Iter;
use std::thread::{spawn, JoinHandle};
use std::rc::Rc;
use std::fmt;
use std::error::Error;
use time::Duration;
use std::sync::mpsc::Receiver;
use token_scheduler::{Scheduler, Abort, AbortableWait, AbortableWaitError, SteadyTimeSource};

use sender::{Collect, Collector};
use producer::ProducerEvent;

#[allow(dead_code)]
pub enum RunMode {
    SharedThread,
    //DedicatedThread,
    //DedicatedProcess
}

pub struct ProbeRunPlan {
    every: Duration,
    probe: Rc<Probe>
}

pub trait Probe: Send {
    fn name(&self) -> &str;
    fn run(&self, collector: &mut Collect) -> Result<(), String>;
    fn run_mode(&self) -> RunMode;
}

pub trait Module {
    fn name(&self) -> &str;
    fn schedule(&self) -> Iter<ProbeRunPlan>;
}

pub struct SharedThreadProbeRunner {
    probes: Vec<Rc<Probe>>
}

impl SharedThreadProbeRunner {
    pub fn new() -> SharedThreadProbeRunner {
        SharedThreadProbeRunner {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: Rc<Probe>) {
        self.probes.push(probe);
    }

    pub fn run(self, collector: &mut Collect) -> Vec<Result<(), String>> {
        self.probes.into_iter().map(|probe| probe.run(collector)).collect()
    }
}

pub struct ProbeScheduler {
    scheduler: Scheduler<Rc<Probe>, SteadyTimeSource>,
    overrun: u64
}

#[derive(Debug)]
enum ProbeSchedulerError {
    Empty,
    Aborted
}

impl fmt::Display for ProbeSchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ProbeSchedulerError::Empty => write!(f, "{}: Scheduler is empty", self.description()),
            &ProbeSchedulerError::Aborted => write!(f, "{}: Scheduler wait has been aborted", self.description())
        }
    }
}

impl Error for ProbeSchedulerError {
    fn description(&self) -> &str {
        "Probe schedule error"
    }
}

impl ProbeScheduler {
    pub fn new() -> ProbeScheduler {
        ProbeScheduler {
            scheduler: Scheduler::new(Duration::milliseconds(100)),
            overrun: 0
        }
    }

    pub fn schedule<'m>(&mut self, module: &'m Module) {
        for probe_schedule in module.schedule() {
            self.scheduler.every(probe_schedule.every, probe_schedule.probe.clone());
        }
    }

    pub fn abort_handle(&self) -> <SteadyTimeSource as AbortableWait>::AbortHandle {
        self.scheduler.abort_handle()
    }

    pub fn abortable_wait(&mut self) -> Result<Vec<Rc<Probe>>, ProbeSchedulerError> {
         match self.scheduler.abortable_wait() {
             Err(AbortableWaitError::Overrun(probe_runs)) => {
                 //TODO: trace each overrun
                 self.overrun = self.overrun + probe_runs.len() as u64;
                 warn!("{} probes overrun their scheduled run time; overruns since start: {}", probe_runs.len(), self.overrun);
                 self.abortable_wait()
             },
             Err(AbortableWaitError::Empty) => Err(ProbeSchedulerError::Empty),
             Err(AbortableWaitError::Aborted) => Err(ProbeSchedulerError::Aborted),
             Ok(probes) => {
                 Ok(probes)
             }
         }
    }

    #[allow(dead_code)]
    pub fn overrun(&self) -> u64 {
        self.overrun
    }
}

mod hello_world;

pub fn start(collector: Collector, events: Receiver<ProducerEvent>) -> JoinHandle<()> {
    spawn(move || {
        let mut ps = ProbeScheduler::new();

        let mut modules: Vec<Box<Module>> = vec![];

        modules.push(hello_world::init());

        for module in modules {
            ps.schedule(&*module);
        }

        //TODO: load modules
        //TODO: schedule modules

        let abort_handle = ps.abort_handle();

        spawn(move || {
            loop {
                match events.recv().expect("master thread died") {
                    ProducerEvent::Hello => debug!("hello received"),
                    ProducerEvent::Shutdown => {
                        info!("Shutting down probe module: aborting scheduler");
                        abort_handle.abort();
                        return
                    }
                }
            }
        });

        loop {
            match ps.abortable_wait() {
                Err(ProbeSchedulerError::Empty) => panic!("no probes configured to run"), //TODO: stop with nice msg
                Err(ProbeSchedulerError::Aborted) => {
                    info!("Scheduler aborted; exiting");
                    return
                }
                Ok(probes) => {
                    let mut run_collector = collector.clone();
                    let mut shared_exec = SharedThreadProbeRunner::new();

                    for probe in probes {
                        match probe.run_mode() {
                            RunMode::SharedThread => shared_exec.push(probe)
                        }
                    }

                    for error in shared_exec.run(&mut run_collector).into_iter().filter(|r| r.is_err()).map(|r| r.unwrap_err()) {
                        error!("Probe reported an error: {}", error);
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use super::ProbeSchedulerError;
    use sender::Collect;
    use messaging::DataValue;
    use time::Duration;
    use std::slice::Iter;
    use std::rc::Rc;

    struct StubModule {
        name: String,
        schedule: Vec<ProbeRunPlan>
    }

    struct StubProbe {
        name: String
    }

    impl StubProbe {
        fn new(name: &str) -> Rc<Self> {
            Rc::new(
                StubProbe {
                    name: name.to_string()
                }
            )
        }
    }

    impl Probe for StubProbe {
        fn name(&self) -> &str {
            &self.name
        }

        fn run(&self, collector: &mut Collect) -> Result<(), String> {
            let mut collector = collector;
            collector.collect("foo", &self.name, "c1", DataValue::Text(format!("{}-{}", self.name, "c1")));
            collector.collect("bar", &self.name, "c2", DataValue::Text(format!("{}-{}", self.name, "c2")));
            Ok(())
        }

        fn run_mode(&self) -> RunMode {
            RunMode::SharedThread
        }
    }

    impl StubModule {
        fn new(name: &str) -> StubModule {
            StubModule {
                name: name.to_string(),
                schedule: Vec::new()
            }
        }

        fn add_schedule(&mut self, every: Duration, probe: Rc<Probe>) {
            self.schedule.push(
                ProbeRunPlan {
                    every: every,
                    probe: probe
                }
            );
        }
    }

    impl Module for StubModule {
        fn name(&self) -> &str {
            &self.name
        }

        fn schedule(&self) -> Iter<ProbeRunPlan> {
            self.schedule.iter()
        }
    }

    struct StubCollector {
        pub values: Vec<DataValue>
    }

    impl Collect for StubCollector {
        fn collect(&mut self, _location: &str, _path: &str, _component: &str, value: DataValue) -> () {
            self.values.push(value);
        }
    }

    impl StubCollector {
        fn text_values(self) -> Vec<String> {
            self.values.into_iter().map(|v| if let DataValue::Text(c) = v { c } else { "none".to_string() }).collect()
        }
    }

    #[test]
    fn shared_thread_probe_executor() {
        let p1 = StubProbe::new("p1");
        let p2 = StubProbe::new("p2");
        let p3 = StubProbe::new("p3");

        let mut exec = SharedThreadProbeRunner::new();
        exec.push(p1);
        exec.push(p2);
        exec.push(p3);

        let mut collector = StubCollector { values: Vec::new() };
        exec.run(&mut collector);

        assert_eq!(collector.text_values(), vec![
           "p1-c1", "p1-c2",
           "p2-c1", "p2-c2",
           "p3-c1", "p3-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_abortable_wait_should_provide_porbes_according_to_schedule() {
        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(100), StubProbe::new("m2-p1"));
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p2"));


        let mut ps: ProbeScheduler = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        let mut collector = StubCollector { values: Vec::new() };
        let probes = ps.abortable_wait().unwrap();
        for probe in probes {
            probe.run(&mut collector).unwrap();
        }

        assert_eq!(collector.text_values(), vec![
           "m1-p1-c1", "m1-p1-c2",
           "m2-p1-c1", "m2-p1-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_abortable_wait_should_count_overrun_schedules() {
        use std::thread::sleep;
        use std::time::Duration as StdDuration;

        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p2"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p1"));

        let mut ps: ProbeScheduler = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        sleep(StdDuration::from_millis(200));

        {
            let result = ps.abortable_wait();
            assert!(result.is_ok());

            let mut collector = StubCollector { values: Vec::new() };
            let probes = result.unwrap();
            for probe in probes {
                probe.run(&mut collector).unwrap();
            }

            assert_eq!(collector.text_values(), vec![
               "m2-p1-c1", "m2-p1-c2",
               "m1-p1-c1", "m1-p1-c2",
               "m1-p2-c1", "m1-p2-c2"
            ]);
        }

        assert_eq!(ps.overrun(), 2);
    }

    #[test]
    fn probe_scheduler_abortable_wait_should_return_abort_on_abort() {
        use std::thread::{spawn, sleep};
        use std::time::Duration as StdDuration;
        use token_scheduler::{Abort, AbortableWait};

        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(1000), StubProbe::new("m1-p1"));
        m1.add_schedule(Duration::milliseconds(1000), StubProbe::new("m1-p2"));

        let mut ps: ProbeScheduler = ProbeScheduler::new();
        let abort_handle = ps.abort_handle();

        ps.schedule(&m1);

        spawn(move || {
            sleep(StdDuration::from_millis(100));
            abort_handle.abort();
        });

        if let Err(ProbeSchedulerError::Aborted) = ps.abortable_wait() {
            // OK
        } else {
            panic!("expected scheduler to be aborted")
        }
    }
}

