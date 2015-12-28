use collector::Collect;
//use asynchronous::{Deferred, ControlFlow};
use time::Duration;
use token_scheduler::{Scheduler, SteadyTimeSource, WaitError};
use std::collections::HashMap;

#[allow(dead_code)]
pub enum ProbeRunMode {
    SharedThread,
    //DedicatedThread,
    //DedicatedProcess
}

//TODO: ProbeId From<&str>
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ModuleId(String);
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ProbeId(String);

#[derive(Clone)]
pub struct ProbeSchedule {
    every: Duration,
    probe: ProbeId
}

pub trait Module<C> where C: Collect {
    fn id(&self) -> ModuleId;
    fn schedule(&self) -> Vec<ProbeSchedule>;
    fn probe(&self, probe: &ProbeId) -> Option<&Probe<C>>;
}

pub trait Probe<C>: Send where C: Collect {
    fn run(&self, collector: &mut C) -> Result<(), String>;
    fn run_mode(&self) -> ProbeRunMode;
}

pub struct SharedThreadProbeExecutor<'a, C> where C: Collect + 'a {
    probes: Vec<&'a Probe<C>>
}

impl<'a, C> SharedThreadProbeExecutor<'a, C> where C: Collect + 'a {
    pub fn new() -> SharedThreadProbeExecutor<'a, C> {
        SharedThreadProbeExecutor {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: &'a Probe<C>) {
        self.probes.push(probe);
    }

    pub fn run(self, collector: &mut C) -> Vec<Result<(), String>> {
        self.probes.into_iter().map(|probe| probe.run(collector)).collect()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ProbeRun(ModuleId, ProbeId);

pub struct ProbeScheduler<'m, C> where C: Collect + 'm {
    scheduler: Scheduler<ProbeRun, SteadyTimeSource>,
    modules: HashMap<ModuleId, &'m Module<C>>,
    missed: u32,
    gone: u32
}

impl<'m, C> ProbeScheduler<'m, C> where C: Collect {
    pub fn new() -> ProbeScheduler<'m, C> {
        ProbeScheduler {
            scheduler: Scheduler::new(Duration::milliseconds(100)),
            modules: HashMap::new(),
            missed: 0,
            gone: 0
        }
    }

    pub fn push(&mut self, module: &'m Module<C>) {
        for probe_schedule in module.schedule() {
            self.scheduler.every(probe_schedule.every, ProbeRun(module.id(), probe_schedule.probe));
        }

        self.modules.insert(module.id(), module);
    }

    //TODO: log and count missed
    //TODO: re-run when all probse were canceled
    //TODO: add stats for missed/not found schedules
    pub fn wait(&mut self) -> Result<Vec<&Probe<C>>, WaitError<ProbeRun>> {
        self.scheduler.wait()
        .map(|probe_runs|
            probe_runs.into_iter()
            .filter_map(|ref probe_run| {
                let &ProbeRun(ref module_id, ref probe_id) = probe_run;
                self.modules.get(module_id).expect(&format!("no module of ID {:?} found", module_id))
                .probe(probe_id).or_else(|| {
                    self.gone = self.gone + 1;
                    //TODO: log: format!("no probe of ID {:?} found in module of ID {:?}", probe_id, module.id()))
                    self.scheduler.cancel(probe_run);
                    None
                })
            }).collect()
        )
    }

    pub fn missed(&self) -> u32 {
        self.missed
    }

    pub fn gone(&self) -> u32 {
        self.gone
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use collector::Collect;
    use messaging::DataValue;
    use std::collections::HashMap;
    use time::Duration;

    struct StubModule {
        id: ModuleId,
        probes: HashMap<ProbeId, StubProbe>,
        schedule: Vec<ProbeSchedule>
    }

    struct StubProbe {
        name: String
    }

    impl StubProbe {
        fn new(name: &str) -> Self {
            StubProbe {
                name: name.to_string()
            }
        }
    }

    impl<C> Probe<C> for StubProbe where C: Collect {
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

    impl StubModule {
        fn new(id: ModuleId) -> StubModule {
            StubModule {
                id: id,
                probes: HashMap::new(),
                schedule: Vec::new()
            }
        }

        fn add_probe(&mut self, id: ProbeId, probe: StubProbe) {
            self.probes.insert(id, probe);
        }

        fn add_schedule(&mut self, every: Duration, probe: ProbeId) {
            self.schedule.push(
                ProbeSchedule {
                    every: every,
                    probe: probe
                }
            );
        }
    }

    impl<C> Module<C> for StubModule where C: Collect {
        fn id(&self) -> ModuleId {
            self.id.clone()
        }

        fn schedule(&self) -> Vec<ProbeSchedule> {
            self.schedule.clone()
        }

        fn probe(&self, probe: &ProbeId) -> Option<&Probe<C>> {
            self.probes.get(probe).map(|p| p as &Probe<C>)
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
        let mut m1 = StubModule::new(ModuleId("m1".to_string()));
        m1.add_probe(ProbeId("p1".to_string()), StubProbe::new("m1-p1"));
        m1.add_probe(ProbeId("p2".to_string()), StubProbe::new("m1-p2"));

        let mut m2 = StubModule::new(ModuleId("m2".to_string()));
        m2.add_probe(ProbeId("p1".to_string()), StubProbe::new("m2-p1"));
        m2.add_probe(ProbeId("p2".to_string()), StubProbe::new("m2-p2"));

        let mut exec = SharedThreadProbeExecutor::new();
        exec.push(m1.probe(&ProbeId("p1".to_string())).unwrap());
        exec.push(m2.probe(&ProbeId("p1".to_string())).unwrap());
        exec.push(m2.probe(&ProbeId("p2".to_string())).unwrap());

        let mut collector = StubCollector { values: Vec::new() };
        exec.run(&mut collector);

        assert_eq!(collector.text_values(), vec![
           "m1-p1-c1", "m1-p1-c2",
           "m2-p1-c1", "m2-p1-c2",
           "m2-p2-c1", "m2-p2-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_wait_should_provide_porbes_according_to_schedule() {
        let mut m1 = StubModule::new(ModuleId("m1".to_string()));
        m1.add_probe(ProbeId("p1".to_string()), StubProbe::new("m1-p1"));
        m1.add_probe(ProbeId("p2".to_string()), StubProbe::new("m1-p2"));
        m1.add_schedule(Duration::milliseconds(100), ProbeId("p1".to_string()));

        let mut m2 = StubModule::new(ModuleId("m2".to_string()));
        m2.add_probe(ProbeId("p1".to_string()), StubProbe::new("m2-p1"));
        m2.add_probe(ProbeId("p2".to_string()), StubProbe::new("m2-p2"));
        m2.add_schedule(Duration::milliseconds(100), ProbeId("p1".to_string()));

        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.push(&m1);
        ps.push(&m2);

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
    fn probe_scheduler_wait_should_handle_missing_probe() {
        let mut m1 = StubModule::new(ModuleId("m1".to_string()));
        m1.add_schedule(Duration::milliseconds(100), ProbeId("p1".to_string()));

        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.push(&m1);

        assert_eq!(ps.gone(), 0);
        {
            let result = ps.wait();
            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
        assert_eq!(ps.gone(), 1);

        let result = ps.wait();
        assert!(result.is_err());
    }
}
