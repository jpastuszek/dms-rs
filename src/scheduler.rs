use std::ops::Fn;
use collector::Collector;

struct Scheduler {
    tasks: Vec<Task>
}

pub struct Task {
    interval: f32,
    task: Box<Fn(&mut Collector) -> ()>
}

impl Task {
    fn new(interval: f32, task: Box<Fn(&mut Collector) -> ()>) -> Task {
        Task {
            interval: interval,
            task: task
        }
    }

    fn run(&self, collector: &mut Collector) -> () {
        (self.task)(collector);
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use collector::{Collector, CollectorThread};
    pub use messaging::*;
    pub use nanomsg::{Socket, Protocol};
    pub use std::io::Read;

    describe! task {
        it "should be crated with closure representing the task that gets collector" {
            let mut pull = Socket::new(Protocol::Pull).unwrap();
            let mut _endpoint = pull.bind("ipc:///tmp/test-collector.ipc").unwrap();
            {
                let task = Task::new(1.0, Box::new(|collector| {
                    collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
                }));

                let collector_thread = CollectorThread::spawn("ipc:///tmp/test-collector.ipc");
                let mut collector = collector_thread.new_collector();

                task.run(&mut collector);

                let mut msg = Vec::new();
                pull.read_to_end(&mut msg).unwrap();
                let msg_string = String::from_utf8_lossy(&msg);
                assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
                assert!(msg_string.contains("myserver"));
            }
        }
    }
}
