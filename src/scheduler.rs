use std::fmt;
use std::ops::Fn;
use chrono::{DateTime, UTC, Duration};
use std::collections::BTreeMap;
use std::cmp::Ordering;
#[cfg(not(test))]
use std::thread::sleep_ms;

pub type Action<C, O, E> = Box<Fn(&mut C) -> Result<O, E>>;

enum TaskBond {
    OneOff,
    Perpetual
}

struct Task<C, O, E> {
    interval: Duration,
    run_offset: DateTime<UTC>,
    task: Action<C, O, E>,
    bond: TaskBond
}

impl<C, O, E> fmt::Debug for Task<C, O, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task({} +{})", self.run_offset, self.interval.num_milliseconds())
    }
}

impl<C, O, E> Task<C, O, E> {
    fn new(interval: Duration, run_offset: DateTime<UTC>, bond: TaskBond, task: Action<C, O, E>) -> Task<C, O, E> {
        assert!(interval > Duration::seconds(0)); // negative interval would make schedule go back in time!
        Task {
            interval: interval,
            run_offset: run_offset,
            task: task,
            bond: bond
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

#[cfg(not(test))]
pub struct RealTimeSource;

#[cfg(not(test))]
impl RealTimeSource {
    fn new() -> RealTimeSource {
        RealTimeSource
    }
}

#[cfg(not(test))]
impl TimeSource for RealTimeSource {
    fn now(&self) -> DateTime<UTC> {
        UTC::now()
    }

    fn wait(&mut self, duration: Duration) {
        sleep_ms(duration.num_milliseconds() as u32)
    }
}

type RunGroup = u32;

// TODO: make this a real error
#[derive(Debug, Eq, PartialEq)]
pub enum SchedulerRunError {
    ScheduleEmpty,
    TasksSkipped(RunGroup)
}

#[derive(Debug, Eq, PartialEq)]
pub enum RunAction {
    None,
    Wait(Duration),
    Skip(Vec<RunGroup>),
    Run(RunGroup)
}

pub struct Scheduler<C, O, E, T> where T: TimeSource {
    offset: DateTime<UTC>,
    group_interval: Duration,
    tasks: BTreeMap<RunGroup, Vec<Task<C, O, E>>>,
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

    fn schedule(&mut self, mut task: Task<C, O, E>) {
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

    pub fn after(&mut self, duration: Duration, action: Action<C, O, E>) {
        self.schedule(Task::new(duration, UTC::now(), TaskBond::OneOff, action));
    }

    pub fn every(&mut self, duration: Duration, action: Action<C, O, E>) {
        self.schedule(Task::new(duration, UTC::now(), TaskBond::Perpetual, action));
    }

    fn run_action(&self) -> RunAction {
        let now = self.time_source.now();
        let current_schedule = self.to_run_group(now - self.offset);

        match self.tasks.iter().next() {
            None => RunAction::None,
            Some((&run_group, _)) => {
                match run_group.cmp(&current_schedule) {
                    Ordering::Greater => RunAction::Wait((self.offset + self.to_duration(run_group)) - now),
                    Ordering::Less => RunAction::Skip(self.tasks.iter().take_while(|&(&run_group, &_)| run_group < current_schedule).map(|(run_group, _)| run_group.clone()).collect()),
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
                    sum + self.tasks[run_group].len() as RunGroup
                });

                for run_group in run_groups {
                    self.tasks.remove(&run_group);
                }
                Err(SchedulerRunError::TasksSkipped(count))
            },
            RunAction::Run(run_group) => {
                let mut out;
                {
                    let run_tasks = self.tasks.get_mut(&run_group).unwrap();
                    run_tasks.sort_by(|a, b| a.run_offset.cmp(&b.run_offset));
                    //println!("{:?}", run_tasks);
                    out = Vec::with_capacity(run_tasks.len());

                    for run_task in run_tasks {
                        out.push(run_task.run(collector));
                    }
                }

                let run_tasks = self.tasks.remove(&run_group).unwrap();
                for run_task in run_tasks.into_iter() {
                    match run_task.bond {
                        TaskBond::Perpetual => self.schedule(run_task), // taks is moved into another vector
                        TaskBond::OneOff => () // the task gets destoryed here
                    };
                }

                Ok(out)
            }
        }
    }

    #[allow(dead_code)]
    pub fn time_source(&self) -> &TimeSource {
        &self.time_source
    }

    fn to_run_group(&self, duration: Duration) -> RunGroup {
        let interval = self.group_interval.num_microseconds().unwrap();
        let duration = duration.num_microseconds().unwrap();
        if duration < 0 || interval <= 0 {
            return 0;
        }

        let run_group = duration / interval;
        if duration % interval != 0 {
            run_group + 1; // ceil
        }
        run_group as RunGroup
    }

    fn to_duration(&self, run_group: RunGroup) -> Duration {
        self.group_interval * (run_group as i32)
    }
}

#[cfg(test)]
mod test {
    mod task {
        use super::super::{Task, TaskBond};
        use chrono::{UTC, Duration};

        #[test]
        fn crated_with_closure_gets_passed_collector() {
            let task: Task<Vec<u8>, (), ()> = Task::new(Duration::seconds(1), UTC::now(), TaskBond::OneOff, Box::new(|collector| {
                collector.push(1);
                collector.push(2);
                Ok(())
            }));

            let mut collector = Vec::new();

            let _ = task.run(&mut collector);
            let _ = task.run(&mut collector);

            assert_eq!(collector, vec![1, 2, 1, 2]);
        }

        #[test]
        fn crated_with_closure_returns_value() {
            let task: Task<(), u8, ()> = Task::new(Duration::seconds(1), UTC::now(), TaskBond::OneOff, Box::new(|_| {
                Ok(1)
            }));

            let mut out = Vec::new();

            out.push(task.run(&mut ()));
            out.push(task.run(&mut ()));

            assert_eq!(out, vec![Ok(1), Ok(1)]);
        }

        #[test]
        fn provides_next_run_schedule() {
            let mut task: Task<(), (), ()> = Task::new(Duration::seconds(42), UTC::now(), TaskBond::OneOff, Box::new(|_| { Ok(()) }));

            assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
            assert_eq!(task.next_schedule() + Duration::seconds(42), task.next_schedule());
        }
    }

    mod scheduler {
        pub use super::super::*;
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

        macro_rules! subject {
            () => {{
                let mut scheduler: Scheduler<(), u8, (), _> = Scheduler::new(Duration::milliseconds(500), FakeTimeSource::new());

                scheduler.after(Duration::milliseconds(1000), Box::new(|_| {
                    Ok(1)
                }));
                scheduler.after(Duration::milliseconds(1100), Box::new(|_| {
                    Ok(2)
                }));
                scheduler.after(Duration::milliseconds(2100), Box::new(|_| {
                    Ok(3)
                }));
                scheduler.after(Duration::milliseconds(2500), Box::new(|_| {
                    Ok(4)
                }));

                scheduler
            }}
        }

        mod run_action {
            pub use super::*;

            #[test]
            fn should_return_wait_with_duration_if_there_is_nothing_to_do_yet() {
                let mut scheduler = subject!();
                assert_eq!(scheduler.run_action(), RunAction::Wait(Duration::seconds(1)));
                scheduler.time_source.wait(Duration::milliseconds(700));
                assert_eq!(scheduler.run_action(), RunAction::Wait(Duration::milliseconds(300)));
            }

            #[test]
            fn should_return_run_with_run_group_when_time_is_just_right() {
                let mut scheduler = subject!();
                scheduler.time_source.wait(Duration::milliseconds(1000));
                assert_eq!(scheduler.run_action(), RunAction::Run(2));
            }

            #[test]
            fn should_return_run_with_run_group_when_time_is_still_within_same_run_group() {
                let mut scheduler = subject!();
                scheduler.time_source.wait(Duration::milliseconds(1499));
                assert_eq!(scheduler.run_action(), RunAction::Run(2));
            }

            #[test]
            fn should_return_skip_with_list_of_skipped_run_groups_when_time_is_after_first_run_group() {
                let mut scheduler = subject!();
                scheduler.time_source.wait(Duration::milliseconds(1500));
                assert_eq!(scheduler.run_action(), RunAction::Skip(vec![2]));
                scheduler.time_source.wait(Duration::milliseconds(1500));
                assert_eq!(scheduler.run_action(), RunAction::Skip(vec![2, 4, 5]));
            }
        }

        #[test]
        fn should_run_tasks_in_order_of_their_actual_run_time() {
            let mut scheduler = subject!();
            scheduler.after(Duration::milliseconds(1050), Box::new(|_| {
                Ok(5)
            }));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(1), Ok(5), Ok(2)])); // 1: 1000, 5: 1050, 2: 1100
        }

        #[test]
        fn should_execute_tasks_given_time_progress_until_empty() {
            let mut scheduler = subject!();
            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(1), Ok(2)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(3)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(4)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Err(SchedulerRunError::ScheduleEmpty));
        }

        #[test]
        fn should_continue_running_if_it_has_a_perpetual_task_scheduled() {
            let mut scheduler = subject!();
            scheduler.every(Duration::milliseconds(1010), Box::new(|_| {
                Ok(5)
            }));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(1), Ok(5), Ok(2)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(3), Ok(5)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(4)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(5)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(5)]));
        }

        #[test]
        fn should_report_skipped_tasks_if_time_progresses_over_run_group() {
            let mut scheduler = subject!();
            scheduler.time_source.wait(Duration::milliseconds(1500));
            let out = scheduler.run(&mut ());
            assert_eq!(out, Err(SchedulerRunError::TasksSkipped(2)));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(3)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Ok(vec![Ok(4)]));

            let out = scheduler.run(&mut ());
            assert_eq!(out, Err(SchedulerRunError::ScheduleEmpty));
        }
    }
}

