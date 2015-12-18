use collector::{CollectorThread, Collector};
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

pub trait Module {
    fn id(&self) -> ModuleId;
    fn schedule(&self) -> Vec<ProbeSchedule>;
    fn probe(&self, probe: ProbeId) -> Option<&Probe>;
}

pub trait Probe: Send {
    fn run(&self, collector: Collector) -> Result<(), String>;
    fn run_mode(&self) -> ProbeRunMode;
}

pub struct ProbeRun(ModuleId, ProbeId);

pub struct SharedThreadProbeExecutor<'a> {
    probes: Vec<&'a Probe>
}

impl<'a> SharedThreadProbeExecutor<'a> {
    pub fn new() -> SharedThreadProbeExecutor<'a> {
        SharedThreadProbeExecutor {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: &'a Probe) {
        self.probes.push(probe);
    }

    pub fn run(self, collector_thread: &CollectorThread) -> Vec<Result<(), String>> {
        //TODO: get collector and clone it?
        self.probes.into_iter().map(|probe| probe.run(collector_thread.new_collector())).collect()
    }
}

//struct ProbeRunner { }

#[cfg(test)]
mod test {
    use super::*;
    use collector::{CollectorThread, Collector};
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

    impl Probe for StubProbe {
        fn run(&self, collector: Collector) -> Result<(), String> {
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

    impl Module for StubModule {
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

        fn probe(&self, probe: ProbeId) -> Option<&Probe> {
            self.probes.get(&probe).map(|p| p as &Probe)
        }
    }

    #[test]
    fn shared_thread_probe_executor() {
        let m1 = StubModule::new(ModuleId("m1".to_string()));
        let m2 = StubModule::new(ModuleId("m2".to_string()));

        let mut exec = SharedThreadProbeExecutor::new();

        //TODO: make generic
        let collector_thread = CollectorThread::spawn("ipc:///tmp/test-collector.ipc");

        exec.push(m1.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe1".to_string())).unwrap());
        exec.push(m2.probe(ProbeId("Probe2".to_string())).unwrap());

        exec.run(&collector_thread);
    }
}
