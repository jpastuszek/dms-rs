use std::fmt;
use std::ops::Fn;
use chrono::{DateTime, UTC, Duration};
use std::collections::BTreeMap;
use std::collections::Bound::{Unbounded, Excluded};
use std::cmp::Ordering;
use std::thread::sleep_ms;

pub struct Task<C, O, E> {
    interval: Duration,
    run_offset: DateTime<UTC>,
    task: Box<Fn(&mut C) -> Result<O, E>>
}

impl<C, O, E> fmt::Debug for Task<C, O, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task({} +{})", self.run_offset, self.interval.num_milliseconds())
    }
}

impl<C, O, E> Task<C, O, E> {
    fn new(interval: Duration, run_offset: DateTime<UTC>, task: Box<Fn(&mut C) -> Result<O, E>>) -> Task<C, O, E> {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            task: task
        }
    }

    fn run(&self, collector: &mut C) -> Result<O, E> {
        (self.task)(collector)
    }

    fn next_schedule(&mut self) -> DateTime<UTC> {
        self.run_offset = self.run_offset + self.interval;
        self.run_offset
    }
}

pub trait TimeSource {
    fn now(&self) -> DateTime<UTC>;
    fn wait(&mut self, duration: Duration);
}

pub struct RealTimeSource;

impl RealTimeSource {
    fn new() -> RealTimeSource {
        RealTimeSource
    }
}

impl TimeSource for RealTimeSource {
    fn now(&self) -> DateTime<UTC> {
        UTC::now()
    }

    fn wait(&mut self, duration: Duration) {
        sleep_ms(duration.num_milliseconds() as u32)
    }
}

#[derive(Debug)]
pub enum SchedulerRunError {
    ScheduleEmpty,
    TasksSkipped(u32)
}

#[derive(Debug, Eq, PartialEq)]
pub enum RunAction {
    None,
    Wait(Duration),
    Skip(Vec<u32>),
    Run(u32)
}

pub struct Scheduler<C, O, E, T> where T: TimeSource {
    offset: DateTime<UTC>,
    group_interval: Duration,
    tasks: BTreeMap<u32, Vec<Task<C, O, E>>>,
    time_source: T
}

impl<C, O, E, T> Scheduler<C, O, E, T> where T: TimeSource {
    pub fn new(group_interval: Duration, time_source: T) -> Scheduler<C, O, E, T>
        where T: TimeSource {
        Scheduler {
            offset: time_source.now(),
            group_interval: group_interval,
            tasks: BTreeMap::new(),
            time_source: time_source
        }
    }

    pub fn schedule(&mut self, mut task: Task<C, O, E>) {
        let now = self.time_source.now();
        let current_schedule = self.to_run_group(now - self.offset);

        let mut schedule;
        loop {
            schedule = self.to_run_group(task.next_schedule() - now);
            if schedule > current_schedule {
                break;
            }
        }

        self.tasks.entry(schedule).or_insert(Vec::new()).push(task);
        //println!("{:?}", self.tasks);
    }

    pub fn run_action(&self) -> RunAction {
        let now = self.time_source.now();
        let current_schedule = self.to_run_group(now - self.offset);

        match self.tasks.iter().next() {
            None => RunAction::None,
            Some((&run_group, _)) => {
                match run_group.cmp(&current_schedule) {
                    Ordering::Greater => RunAction::Wait((self.offset + self.to_duration(run_group)) - now),
                    Ordering::Less => RunAction::Skip(self.tasks.range(Unbounded, Excluded(&current_schedule)).map(|(run_group, _)| run_group.clone()).collect()),
                    Ordering::Equal => RunAction::Run(run_group)
                }
            }
        }
    }

    pub fn run(&mut self, collector: &mut C) -> Result<Vec<Result<O, E>>, SchedulerRunError> {
        match self.run_action() {
            RunAction::None => Err(SchedulerRunError::ScheduleEmpty),
            RunAction::Wait(duration) => {
                self.time_source.wait(duration);
                self.run(collector)
            },
            RunAction::Skip(run_groups) => {
                let count = run_groups.iter().fold(0, |sum, run_group| {
                    sum + self.tasks[run_group].len() as u32
                });
                Err(SchedulerRunError::TasksSkipped(count))
            },
            RunAction::Run(run_group) => {
                let mut out;
                {
                    let ref run_tasks = self.tasks[&run_group];
                    out = Vec::with_capacity(run_tasks.len());

                    for run_task in run_tasks {
                        out.push(run_task.run(collector));
                    }
                }
                self.tasks.remove(&run_group);
                Ok(out)
            }
        }
    }

    #[allow(dead_code)]
    pub fn time_source(&self) -> &TimeSource {
        &self.time_source
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

    fn to_duration(&self, run_group: u32) -> Duration {
        self.group_interval * (run_group as i32)
    }
}

#[cfg(test)]
mod test {
    pub use super::*;
    pub use chrono::{DateTime, UTC, Duration};
    pub use std::thread::sleep_ms;

    pub struct FakeTimeSource {
        now: DateTime<UTC>
    }

    impl FakeTimeSource {
        fn new() -> FakeTimeSource {
            FakeTimeSource {
                now: UTC::now()
            }
        }
    }

    impl TimeSource for FakeTimeSource {
        fn now(&self) -> DateTime<UTC> {
            self.now
        }

        fn wait(&mut self, duration: Duration) {
            self.now = self.now + duration;
        }
    }

    describe! task {
        it "should be crated with closure representing the task that gets collector" {
            let task: Task<Vec<u8>, (), ()> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|collector| {
                collector.push(1);
                collector.push(2);
                Ok(())
            }));

            let mut collector = Vec::new();

            let _ = task.run(&mut collector);
            let _ = task.run(&mut collector);

            assert_eq!(collector, vec![1, 2, 1, 2]);
        }

        it "should be crated with closure representing the task that returns something" {
            let task: Task<(), u8, ()> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|_| {
                Ok(1)
            }));

            let mut out = Vec::new();

            out.push(task.run(&mut ()));
            out.push(task.run(&mut ()));

            assert_eq!(out, vec![Ok(1), Ok(1)]);
        }

        describe! next_schedule {
            it "should provide next run schedule for this task" {
                let mut task: Task<(), (), ()> = Task::new(Duration::seconds(42), UTC::now(), Box::new(|_| { Ok(()) }));

                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
                assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
            }
        }
    }

    describe! scheduler {
        describe! run_action {
            before_each {
                let mut scheduler = Scheduler::new(Duration::milliseconds(500), FakeTimeSource::new());

                let task1: Task<(),u8,()> = Task::new(Duration::milliseconds(1000), UTC::now(), Box::new(|_| {
                    Ok(1)
                }));
                let task2: Task<(),u8,()> = Task::new(Duration::milliseconds(1100), UTC::now(), Box::new(|_| {
                    Ok(1)
                }));
                let task3: Task<(),u8,()> = Task::new(Duration::milliseconds(2100), UTC::now(), Box::new(|_| {
                    Ok(1)
                }));
                let task4: Task<(),u8,()> = Task::new(Duration::milliseconds(2500), UTC::now(), Box::new(|_| {
                    Ok(1)
                }));

                scheduler.schedule(task1);
                scheduler.schedule(task2);
                scheduler.schedule(task3);
                scheduler.schedule(task4);
            }

            it "should return Wait with duration if there is nothing to do yet" {
                assert_eq!(scheduler.run_action(), RunAction::Wait(Duration::seconds(1)));
                scheduler.time_source.wait(Duration::milliseconds(700));
                assert_eq!(scheduler.run_action(), RunAction::Wait(Duration::milliseconds(300)));
            }

            it "should return Run with run group when time is just right" {
                scheduler.time_source.wait(Duration::milliseconds(1000));
                assert_eq!(scheduler.run_action(), RunAction::Run(2));
            }

            it "should return Run with run group when time is still within same run group" {
                scheduler.time_source.wait(Duration::milliseconds(1499));
                assert_eq!(scheduler.run_action(), RunAction::Run(2));
            }

            it "should return Skip with list of skipped run groups when time is after first run group" {
                scheduler.time_source.wait(Duration::milliseconds(1500));
                assert_eq!(scheduler.run_action(), RunAction::Skip(vec![2]));
                scheduler.time_source.wait(Duration::milliseconds(1500));
                assert_eq!(scheduler.run_action(), RunAction::Skip(vec![2, 4, 5]));
            }
        }

        it "should execute tasks given time progress" {
            let mut scheduler = Scheduler::new(Duration::milliseconds(500), FakeTimeSource::new());

            let task: Task<(),u8,()> = Task::new(Duration::seconds(1), UTC::now(), Box::new(|_| {
                Ok(1)
            }));

            scheduler.schedule(task);

            let out = scheduler.run(&mut ());
            assert_eq!(out.unwrap(), vec![Ok(1)]);
        }
    }
}

