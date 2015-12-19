use collector::Collect;
use messaging::DataValue;
use asynchronous::{Deferred, ControlFlow};
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
    fn run(&self, collector: C) -> Result<(), String>;
    fn run_mode(&self) -> ProbeRunMode;
}

pub struct ProbeRun(ModuleId, ProbeId);

pub struct SharedThreadProbeExecutor<'a, C> where C: Collect + Clone {
    probes: Vec<&'a Probe<C>>
}

impl<'a, C> SharedThreadProbeExecutor<'a, C> where C: Collect + Clone {
    pub fn new() -> SharedThreadProbeExecutor<'a, C> {
        SharedThreadProbeExecutor {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: &'a Probe<C>) {
        self.probes.push(probe);
    }

    pub fn run(self, collector: &C) -> Vec<Result<(), String>> {
        self.probes.into_iter().map(|probe| probe.run(collector.clone())).collect()
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

    struct StubProbe;

    impl StubProbe {
        //TODO: Self or StubProbe?
        fn new() -> Self {
            StubProbe
        }
    }

    impl<C> Probe<C> for StubProbe where C: Collect {
        fn run(&self, collector: C) -> Result<(), String> {
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
            m.probes.insert(ProbeId("Probe1".to_string()), StubProbe::new());
            m.probes.insert(ProbeId("Probe2".to_string()), StubProbe::new());
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

    #[derive(Clone)]
    struct StubCollector;

    impl Collect for StubCollector {
        fn collect(&mut self, location: &str, path: &str, component: &str, value: DataValue) -> () {
            ()
        }
    }

    #[test]
    fn shared_thread_probe_executor() {
        let m1 = StubModule::new(ModuleId("m1".to_string()));
        let m2 = StubModule::new(ModuleId("m2".to_string()));

        let mut exec = SharedThreadProbeExecutor::new();

        let collector = StubCollector;

        exec.push(m1.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe2".to_string())).unwrap());

        exec.run(&collector);
    }
}
