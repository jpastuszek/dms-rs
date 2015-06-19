use std::fmt;
use std::ops::Fn;
use chrono::{DateTime, UTC, Duration};
use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};

pub struct Task<C,O> {
    interval: Duration,
    run_offset: DateTime<UTC>,
    task: Box<Fn(&mut C) -> O>
}

impl<C,O> fmt::Debug for Task<C,O> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task({}, {})", self.interval, self.run_offset)
    }
}

impl<C,O> Task<C,O> {
    fn new(interval: Duration, run_offset: DateTime<UTC>, task: Box<Fn(&mut C) -> O>) -> Task<C,O> {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            task: task
        }
    }

    fn run(&self, collector: &mut C) -> O {
        (self.task)(collector)
    }

    fn next_schedule(&mut self) -> DateTime<UTC> {
        self.run_offset = self.run_offset + self.interval;
        self.run_offset
    }
}

pub struct Scheduler<C,O> {
    offset: DateTime<UTC>,
    group_interval: Duration,
    tasks: BTreeMap<u32, Vec<Task<C,O>>>
}

impl<C,O> Scheduler<C,O> {
    pub fn new(group_interval: Duration) -> Scheduler<C,O> {
        Scheduler {
            offset: UTC::now(),
            group_interval: group_interval,
            tasks: BTreeMap::new()
        }
    }

    pub fn schedule(&mut self, mut task: Task<C,O>) {
        let now = UTC::now();
        let current_schedule = self.to_run_group(now - self.offset);

        let mut schedule;
        loop {
            schedule = self.to_run_group(task.next_schedule() - now);
            if schedule > current_schedule {
                break;
            }
        }

        self.tasks.entry(schedule).or_insert(Vec::new()).push(task);

        println!("{:?}", self.tasks);
    }

    pub fn run(&mut self, collector: &mut C) -> Vec<O> {
        println!("{:?}", self.tasks);

        let now = UTC::now();
        let current_schedule = self.to_run_group(now - self.offset);
        let mut out;

        let mut run_groups = Vec::new();
        {
            let mut run_tasks = Vec::<&Task<C,O>>::new();

            for (run_group, ref tasks) in self.tasks.range(Unbounded, Included(&current_schedule)) {
                run_tasks.extend(tasks.iter());
                run_groups.push(run_group.clone());
            }

            out = Vec::with_capacity(run_tasks.len());

            for run_task in &run_tasks {
                out.push(run_task.run(collector));
            }

            println!("{:?}", run_tasks);
        }

        for ref run_group in run_groups {
           self.tasks.remove(run_group);
        }
        println!("{:?}", self.tasks);

        out
    }


    fn to_run_group(&self, duration: Duration) -> u32 {
        let interval = self.group_interval.num_microseconds().unwrap();
        let duration = duration.num_microseconds().unwrap();
        if duration < 0 || interval <= 0 {
            return 0;
        }

        let run_group = duration / interval;
        if duration % interval != 0 {
            run_group + 1; // ceil
        }
        run_group as u32
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use chrono::{UTC, Duration};
    pub use std::thread::sleep_ms;

    describe! task {
        it "should be crated with closure representing the task that gets collector" {
            let task: Task<Vec<u8>,()> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|collector| {
                collector.push(1);
                collector.push(2);
            }));

            let mut collector = Vec::new();

            task.run(&mut collector);
            task.run(&mut collector);

            assert_eq!(collector, vec![1, 2, 1, 2]);
        }

        it "should be crated with closure representing the task that returns something" {
            let task: Task<(),u8> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|_| {
                1
            }));

            let mut out = Vec::new();

            out.push(task.run(&mut ()));
            out.push(task.run(&mut ()));

            assert_eq!(out, vec![1, 1]);
        }

        describe! next_schedule {
            it "should provide next run schedule for this task" {
                let mut task: Task<(),()> = Task::new(Duration::seconds(42), UTC::now(), Box::new(|_| { }));

                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
            }
        }
    }

    describe! scheduler {
        it "should allow scheduling tasks at regular intervals that are rounded and grouped together" {
            let mut scheduler = Scheduler::new(Duration::milliseconds(500));

            let task: Task<(),u8> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|_| {
                1
            }));

            scheduler.schedule(task);

            sleep_ms(1500);
            let out = scheduler.run(&mut ());
            assert_eq!(out, vec![1]);
        }
    }
}
