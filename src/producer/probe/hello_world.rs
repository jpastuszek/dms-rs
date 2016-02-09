use std::rc::Rc;
use time::Duration;

use collector::{Collect, Collector};
use messaging::DataValue;
use super::{ProbeRunMode, ProbeSchedule, Probe, Module};

pub struct HelloWorldProbe;
pub struct HelloWorldModule;


impl<C> Probe<C> for HelloWorldProbe where C: Collect {
    fn name(&self) -> &str {
        "hello world probe"
    }

    fn run(&self, collector: &mut C) -> Result<(), String> {
        let mut collector = collector;
        info!("Hello world!");
        collector.collect("hello", "foo", "c1", DataValue::Text("blah".to_string()));
        collector.collect("world", "bar", "c2", DataValue::Integer(42));
        Ok(())
    }

    fn run_mode(&self) -> ProbeRunMode {
        ProbeRunMode::SharedThread
    }
}

impl<C> Module<C> for HelloWorldModule where C: Collect {
    fn name(&self) -> &str {
        "hello world module"
    }

    fn schedule(&self) -> Vec<ProbeSchedule<C>> {
        vec![
            ProbeSchedule {
                every: Duration::milliseconds(1000),
                probe: Rc::new(HelloWorldProbe)
            }
        ]
    }
}

pub fn init() -> Box<Module<Collector>> {
    Box::new(HelloWorldModule)
}


