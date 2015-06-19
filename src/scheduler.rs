use std::fmt;
use std::ops::Fn;
use chrono::{DateTime, UTC, Duration};
use collector::Collector;
use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};

pub struct Task {
    interval: Duration,
    run_offset: DateTime<UTC>,
    task: Box<Fn(&mut Collector) -> ()>
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task({}, {})", self.interval, self.run_offset)
    }
}

impl Task {
    fn new(interval: Duration, run_offset: DateTime<UTC>, task: Box<Fn(&mut Collector) -> ()>) -> Task {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            task: task
        }
    }

    fn run(&self, collector: &mut Collector) -> () {
        (self.task)(collector);
    }

    fn next_schedule(&mut self) -> DateTime<UTC> {
        self.run_offset = self.run_offset + self.interval;
        self.run_offset
    }
}

pub struct Scheduler {
    offset: DateTime<UTC>,
    group_interval: Duration,
    tasks: BTreeMap<u32, Vec<Task>>
}

impl Scheduler {
    pub fn new(group_interval: Duration) -> Scheduler {
        Scheduler {
            offset: UTC::now(),
            group_interval: group_interval,
            tasks: BTreeMap::new()
        }
    }

    pub fn schedule(&mut self, mut task: Task) {
        let now = UTC::now();
        let current_schedule = self.to_quantum(now - self.offset);

        let mut schedule;
        loop {
            schedule = self.to_quantum(task.next_schedule() - now);
            if schedule > current_schedule {
                break;
            }
        }

        self.tasks.entry(schedule).or_insert(Vec::new()).push(task);

        println!("{:?}", self.tasks);
    }

    pub fn run(&mut self, collector: &mut Collector) {
        println!("{:?}", self.tasks);

        let now = UTC::now();
        let current_schedule = self.to_quantum(now - self.offset);

        let mut run_groups = Vec::new();
        {
            let mut run_tasks = Vec::<&Task>::new();

            for (run_group, ref tasks) in self.tasks.range(Unbounded, Included(&current_schedule)) {
                run_tasks.extend(tasks.iter());
                run_groups.push(run_group.clone());
            }

            for run_task in &run_tasks {
                run_task.run(collector);
            }

            println!("{:?}", run_tasks);
        }

        for ref run_group in run_groups {
           self.tasks.remove(run_group);
        }
        println!("{:?}", self.tasks);
    }


    fn to_quantum(&self, duration: Duration) -> u32 {
        let interval = self.group_interval.num_microseconds().unwrap();
        let duration = duration.num_microseconds().unwrap();
        if duration < 0 || interval <= 0 {
            return 0;
        }

        let quantum = duration / interval;
        if duration % interval != 0 {
            quantum + 1; // ceil
        }
        quantum as u32
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use collector::{Collector, CollectorThread};
    pub use messaging::*;
    pub use nanomsg::{Socket, Protocol};
    pub use std::io::Read;
    pub use chrono::{UTC, Duration};
    pub use std::thread::sleep_ms;

    describe! task {
        it "should be crated with closure representing the task that gets collector" {
            let mut pull = Socket::new(Protocol::Pull).unwrap();
            let mut _endpoint = pull.bind("ipc:///tmp/test-scheduler.ipc").unwrap();
            {
                let task = Task::new(Duration::seconds(1), UTC::now(), Box::new(|collector| {
                    collector.collect("myserver", "os/cpu/usage", "user", DataValue::Float(0.4));
                }));

                let collector_thread = CollectorThread::spawn("ipc:///tmp/test-scheduler.ipc");
                let mut collector = collector_thread.new_collector();

                task.run(&mut collector);

                let mut msg = Vec::new();
                pull.read_to_end(&mut msg).unwrap();
                let msg_string = String::from_utf8_lossy(&msg);
                assert!(msg_string.contains("RawDataPoint/\n0\ncapnp\n\n"));
                assert!(msg_string.contains("myserver"));
            }
        }

        describe! next_schedule {
            it "should provide next run schedule for this task" {
                let mut task = Task::new(Duration::seconds(42), UTC::now(), Box::new(|collector| { }));

                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
            }
        }
    }

    describe! scheduler {
        it "should allow scheduling tasks at regular intervals that are rounded and grouped together" {
            let mut scheduler = Scheduler::new(Duration::milliseconds(500));

            let task = Task::new(Duration::seconds(1), UTC::now(), Box::new(|collector| {
                println!("{}: {}", UTC::now(), "hello");
            }));

            scheduler.schedule(task);

            let collector_thread = CollectorThread::spawn("ipc:///tmp/test-scheduler.ipc");
            let mut collector = collector_thread.new_collector();

            sleep_ms(1500);
            scheduler.run(&mut collector);
        }
    }
}

