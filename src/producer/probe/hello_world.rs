use std::rc::Rc;
use std::slice::Iter;
use time::Duration;

use sender::{Collect, Collector};
use messaging::DataValue;
use super::{RunMode, ProbeRunPlan, Probe, Module};

pub struct HelloWorldProbe;
pub struct HelloWorldModule<C> where C: Collect {
    schedule: Vec<ProbeRunPlan<C>>
}


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

    fn run_mode(&self) -> RunMode {
        RunMode::SharedThread
    }
}

impl<C> Module<C> for HelloWorldModule<C> where C: Collect {
    fn name(&self) -> &str {
        "hello world module"
    }

    fn schedule(&self) -> Iter<ProbeRunPlan<C>> {
        self.schedule.iter()
    }
}

pub fn init() -> Box<Module<Collector>> {
    Box::new(HelloWorldModule {
        schedule: vec![
            ProbeRunPlan {
                every: Duration::milliseconds(1000),
                probe: Rc::new(HelloWorldProbe)
            }
        ]
    })
}

