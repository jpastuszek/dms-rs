use std::rc::Rc;
use std::slice::Iter;
use time::Duration;

use sender::Collect;
use messaging::DataValue;
use super::{RunMode, ProbeRunPlan, Probe, Module};

pub struct HelloWorldProbe;
pub struct HelloWorldModule {
    schedule: Vec<ProbeRunPlan>
}

impl Probe for HelloWorldProbe {
    fn name(&self) -> &str {
        "hello world probe"
    }

    fn run(&self, collector: &mut Collect) -> Result<(), String> {
        let mut collector = collector;
        info!("Hello world!");
        collector.collect("hello", "foo", "c1", DataValue::Text("blah".to_string()));
        collector.collect("world", "bar", "c2", DataValue::Integer(42));
        Ok(())
    }

    fn run_mode(&self) -> RunMode {
        RunMode::SharedThread
    }
}

impl Module for HelloWorldModule {
    fn name(&self) -> &str {
        "hello world module"
    }

    fn schedule(&self) -> Iter<ProbeRunPlan> {
        self.schedule.iter()
    }
}

pub fn init() -> Box<Module> {
    Box::new(HelloWorldModule {
        schedule: vec![
            ProbeRunPlan {
                every: Duration::milliseconds(1000),
                probe: Rc::new(HelloWorldProbe)
            }
        ]
    })
}

