use collector::Collect;
//use asynchronous::{Deferred, ControlFlow};
use time::Duration;

#[allow(dead_code)]
pub enum ProbeRunMode {
    SharedThread,
    //DedicatedThread,
    //DedicatedProcess
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct ModuleId(String);
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct ProbeId(String);

pub struct ProbeSchedule {
    every: Duration,
    probe: ProbeId
}

pub trait Module<C> where C: Collect {
    fn id(&self) -> ModuleId;
    fn schedule(&self) -> Vec<ProbeSchedule>;
    fn probe(&self, probe: ProbeId) -> Option<&Probe<C>>;
}

pub trait Probe<C>: Send where C: Collect {
    fn run(&self, collector: &mut C) -> Result<(), String>;
    fn run_mode(&self) -> ProbeRunMode;
}

pub struct ProbeRun(ModuleId, ProbeId);

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

//struct ProbeRunner { }

#[cfg(test)]
mod test {
    use super::*;
    use collector::Collect;
    use messaging::DataValue;
    use std::collections::HashMap;
    use time::Duration;

    struct StubModule {
        id: ModuleId,
        probes: HashMap<ProbeId, StubProbe>
    }

    struct StubProbe {
        num: i64
    }

    impl StubProbe {
        //TODO: Self or StubProbe?
        fn new(num: i64) -> Self {
            StubProbe { num: num }
        }
    }

    impl<C> Probe<C> for StubProbe where C: Collect {
        fn run(&self, collector: &mut C) -> Result<(), String> {
            let mut collector = collector;
            collector.collect("foo", "bar", "baz", DataValue::Integer(self.num));
            collector.collect("foo", "bar", "baz", DataValue::Integer(self.num + 1));
            Ok(())
        }

        fn run_mode(&self) -> ProbeRunMode {
            ProbeRunMode::SharedThread
        }
    }

    impl StubModule {
        fn new(id: ModuleId) -> StubModule {
            let mut m = StubModule {
                id: id,
                probes: HashMap::new()
            };
            //TODO: ProbeId From<&str>
            m.probes.insert(ProbeId("Probe1".to_string()), StubProbe::new(10));
            m.probes.insert(ProbeId("Probe2".to_string()), StubProbe::new(20));
            m
        }
    }

    impl<C> Module<C> for StubModule where C: Collect {
        fn id(&self) -> ModuleId {
            self.id.clone()
        }

        fn schedule(&self) -> Vec<ProbeSchedule> {
            vec![
                ProbeSchedule {
                    every: Duration::seconds(1),
                    probe: ProbeId("Probe1".to_string())
                },
                ProbeSchedule {
                    every: Duration::seconds(4),
                    probe: ProbeId("Probe2".to_string())
                }
            ]
        }

        fn probe(&self, probe: ProbeId) -> Option<&Probe<C>> {
            self.probes.get(&probe).map(|p| p as &Probe<C>)
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

    #[test]
    fn shared_thread_probe_executor() {
        let m1 = StubModule::new(ModuleId("m1".to_string()));
        let m2 = StubModule::new(ModuleId("m2".to_string()));

        let mut exec = SharedThreadProbeExecutor::new();
        exec.push(m1.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe2".to_string())).unwrap());

        let mut collector = StubCollector { values: vec![] };
        exec.run(&mut collector);

        let collected_values: Vec<i64> = collector.values.into_iter().map(|v| if let DataValue::Integer(c) = v { c } else { 0 }).collect();

        assert_eq!(collected_values, vec![10, 11, 10, 11, 20, 21]);
    }
}
