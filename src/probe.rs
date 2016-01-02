use collector::Collect;
//use asynchronous::{Deferred, ControlFlow};
use std::rc::Rc;
use std::fmt;
use std::error::Error;
use time::Duration;
use token_scheduler::{Scheduler, SteadyTimeSource, WaitError};

#[allow(dead_code)]
pub enum ProbeRunMode {
    SharedThread,
    //DedicatedThread,
    //DedicatedProcess
}

pub struct ProbeSchedule<C> where C: Collect {
    every: Duration,
    probe: Rc<Probe<C>>
}

impl<C> Clone for ProbeSchedule<C> where C: Collect {
    fn clone(&self) -> ProbeSchedule<C> {
        ProbeSchedule {
            every: self.every.clone(),
            probe: self.probe.clone()
        }
    }
}

pub trait Probe<C>: Send where C: Collect {
    fn name(&self) -> &str;
    fn run(&self, collector: &mut C) -> Result<(), String>;
    fn run_mode(&self) -> ProbeRunMode;
}

pub trait Module<C> where C: Collect {
    fn name(&self) -> &str;
    fn schedule(&self) -> Vec<ProbeSchedule<C>>;
}

pub struct SharedThreadProbeExecutor<C> where C: Collect {
    probes: Vec<Rc<Probe<C>>>
}

impl<C> SharedThreadProbeExecutor<C> where C: Collect {
    pub fn new() -> SharedThreadProbeExecutor<C> {
        SharedThreadProbeExecutor {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: Rc<Probe<C>>) {
        self.probes.push(probe);
    }

    pub fn run(self, collector: &mut C) -> Vec<Result<(), String>> {
        self.probes.into_iter().map(|probe| probe.run(collector)).collect()
    }
}

pub struct ProbeScheduler<C> where C: Collect + {
    scheduler: Scheduler<Rc<Probe<C>>, SteadyTimeSource>,
    missed: usize
}

#[derive(Debug)]
struct EmptySchedulerError;

impl fmt::Display for EmptySchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduler is empty")
    }
}

impl Error for EmptySchedulerError {
    fn description(&self) -> &str {
        "probe schedule error"
    }
}

impl<C> ProbeScheduler<C> where C: Collect {
    pub fn new() -> ProbeScheduler<C> {
        ProbeScheduler {
            scheduler: Scheduler::new(Duration::milliseconds(100)),
            missed: 0
        }
    }

    pub fn schedule<'m>(&mut self, module: &'m Module<C>) {
        for probe_schedule in module.schedule() {
            self.scheduler.every(probe_schedule.every, probe_schedule.probe);
        }
    }

    pub fn wait(&mut self) -> Result<Vec<Rc<Probe<C>>>, EmptySchedulerError> {
         match self.scheduler.wait() {
             Err(WaitError::Empty) => Err(EmptySchedulerError),
             Err(WaitError::Missed(probe_runs)) => {
                 //TODO: log missed
                 self.missed = self.missed + probe_runs.len();
                 self.wait()
             },
             Ok(probes) => {
                 Ok(probes)
             }
         }
    }

    pub fn missed(&self) -> usize {
        self.missed
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use collector::Collect;
    use messaging::DataValue;
    use time::Duration;
    use std::rc::Rc;

    struct StubModule<C> where C: Collect {
        name: String,
        schedule: Vec<ProbeSchedule<C>>
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

    impl<C> Probe<C> for StubProbe where C: Collect {
        fn name(&self) -> &str {
            &self.name
        }

        fn run(&self, collector: &mut C) -> Result<(), String> {
            let mut collector = collector;
            collector.collect("foo", &self.name, "c1", DataValue::Text(format!("{}-{}", self.name, "c1")));
            collector.collect("bar", &self.name, "c2", DataValue::Text(format!("{}-{}", self.name, "c2")));
            Ok(())
        }

        fn run_mode(&self) -> ProbeRunMode {
            ProbeRunMode::SharedThread
        }
    }

    impl<C> StubModule<C> where C: Collect  {
        fn new(name: &str) -> StubModule<C> {
            StubModule {
                name: name.to_string(),
                schedule: Vec::new()
            }
        }

        fn add_schedule(&mut self, every: Duration, probe: Rc<Probe<C>>) {
            self.schedule.push(
                ProbeSchedule {
                    every: every,
                    probe: probe
                }
            );
        }
    }

    impl<C> Module<C> for StubModule<C> where C: Collect {
        fn name(&self) -> &str {
            &self.name
        }

        fn schedule(&self) -> Vec<ProbeSchedule<C>> {
            self.schedule.clone()
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

        let mut exec = SharedThreadProbeExecutor::new();
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
    fn probe_scheduler_wait_should_provide_porbes_according_to_schedule() {
        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(100), StubProbe::new("m2-p1"));
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p2"));


        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        let result = ps.wait();
        assert!(result.is_ok());

        let mut collector = StubCollector { values: Vec::new() };
        let probes = result.unwrap();
        for probe in probes {
            probe.run(&mut collector).unwrap();
        }

        assert_eq!(collector.text_values(), vec![
           "m1-p1-c1", "m1-p1-c2",
           "m2-p1-c1", "m2-p1-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_wait_should_count_missed_schedules() {
        use std::thread::sleep_ms;

        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p2"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p1"));

        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        sleep_ms(200);

        {
            let result = ps.wait();
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

        assert_eq!(ps.missed(), 2);
    }
}
