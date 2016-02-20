use std::slice::Iter;
use std::sync::mpsc::{channel, Sender};
use std::thread::{spawn, JoinHandle};
use std::rc::Rc;
use std::fmt;
use std::error::Error;
use std::thread::sleep;
use std::time::Duration as StdDuration;
use time::Duration;
use token_scheduler::{Scheduler, Schedule as NextSchedule, SteadyTimeSource};
use carboxyl::{Sink, Stream};

use sender::{Collect, Collector};
use producer::ProducerEvent;

#[allow(dead_code)]
pub enum RunMode {
    SharedThread,
    //DedicatedThread,
    //DedicatedProcess
}

pub struct ProbeRunPlan<C> where C: Collect {
    every: Duration,
    probe: Rc<Probe<C>>
}

pub trait Probe<C>: Send where C: Collect {
    fn name(&self) -> &str;
    fn run(&self, collector: &mut C) -> Result<(), String>;
    fn run_mode(&self) -> RunMode;
}

pub trait Module<C> where C: Collect {
    fn name(&self) -> &str;
    fn schedule(&self) -> Iter<ProbeRunPlan<C>>;
}

pub struct SharedThreadProbeRunner<C> where C: Collect {
    probes: Vec<Rc<Probe<C>>>
}

impl<C> SharedThreadProbeRunner<C> where C: Collect {
    pub fn new() -> SharedThreadProbeRunner<C> {
        SharedThreadProbeRunner {
            probes: Vec::new()
        }
    }

    pub fn push(&mut self, probe: Rc<Probe<C>>) {
        self.probes.push(probe);
    }

    pub fn run(self, collector: &mut C) -> Vec<Result<(), String>> {
        self.probes.into_iter().map(|probe| probe.run(collector)).collect()
    }
}

pub struct ProbeScheduler<C> where C: Collect {
    scheduler: Scheduler<Rc<Probe<C>>, SteadyTimeSource>,
    overrun: u64,
    timer: Timer
}

#[derive(Debug)]
enum ProbeSchedulerError {
    Empty
}

impl fmt::Display for ProbeSchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ProbeSchedulerError::Empty => write!(f, "{}: Scheduler is empty", self.description())
        }
    }
}

impl Error for ProbeSchedulerError {
    fn description(&self) -> &str {
        "Probe schedule error"
    }
}

//TODO: somehow move this to token_scheduler; some way to select on it - behave as IO?
pub enum ProbeSchedule<C> where C: Collect {
    Wait(Stream<Alarm>),
    Probes(Vec<Rc<Probe<C>>>)
}

impl<C> ProbeScheduler<C> where C: Collect {
    pub fn new() -> ProbeScheduler<C> {
        ProbeScheduler {
            scheduler: Scheduler::new(Duration::milliseconds(100)),
            overrun: 0,
            timer: Timer::spawn()
        }
    }

    pub fn schedule<'m>(&mut self, module: &'m Module<C>) {
        for probe_schedule in module.schedule() {
            self.scheduler.every(probe_schedule.every, probe_schedule.probe.clone());
        }
    }

    pub fn next(&mut self) -> Result<ProbeSchedule<C>, ProbeSchedulerError> {
         match self.scheduler.next() {
             Some(NextSchedule::NextIn(duration)) => Ok(ProbeSchedule::Wait(self.timer.alarm_in(duration))),
             Some(NextSchedule::Overrun(probe_runs)) => {
                 //TODO: log overrun
                 self.overrun = self.overrun + probe_runs.len() as u64;
                 warn!("{} probes overrun their scheduled run time; overruns since start: {}", probe_runs.len(), self.overrun);
                 self.next()
             },
             Some(NextSchedule::Current(probes)) => {
                 Ok(ProbeSchedule::Probes(probes))
             }
             None => Err(ProbeSchedulerError::Empty)
         }
    }

    #[allow(dead_code)]
    pub fn overrun(&self) -> u64 {
        self.overrun
    }
}

struct Timer {
    task_send: Sender<(Sink<Alarm>, Duration)>
}

#[derive(Clone)]
struct Alarm;

unsafe impl Sync for Alarm {}
unsafe impl Send for Alarm {}

impl Timer {
    fn spawn() -> Timer {
        let (task_send,  task_recv) = channel();

        spawn(move || {
            loop {
                match task_recv.recv() {
                    Ok((alarm_sink, duration)) => {
                        let duration: Duration = duration;
                        let alarm_sink: Sink<Alarm> = alarm_sink;

                        sleep(StdDuration::new(
                            duration.num_seconds() as u64,
                            (duration.num_nanoseconds().expect("sleep duration too large") - duration.num_seconds() * 1_000_000_000) as u32
                        ));
                        alarm_sink.send(Alarm);
                    }
                    Err(_) => return // client gone
                }
            }
        });

        Timer {
            task_send: task_send
        }
    }

    fn alarm_in(&self, duration: Duration) -> Stream<Alarm> {
        let alarm_sink = Sink::new();
        let alarm_stream = alarm_sink.stream();
        self.task_send.send((alarm_sink, duration)).expect("Timer tread died!");
        alarm_stream
    }
}

#[derive(Clone)]
enum ProbeLoopEvents {
    ProducerEvent(ProducerEvent),
    Timer(Alarm)
}

mod hello_world;

pub fn start(collector: Collector, events: Stream<ProducerEvent>) -> JoinHandle<()> {
    spawn(move || {
        let mut ps = ProbeScheduler::new();

        let mut modules: Vec<Box<Module<Collector>>> = vec![];

        modules.push(hello_world::init());

        for module in modules {
            ps.schedule(&*module);
        }

        //TODO: load modules
        //TODO: schedule modules
        //TODO: need a way to stop the thread
        // 1 use park_timeout as slee funciton and check some mutex or channel if we were asekd to
        //   exit - this requires collector to send message or rais signal and then unpark
        // 2 use Condvar with wait_timeout
        // 3 use separate thread that will sleep and notify on channel when done - select on event
        //   and it and use it to wait https://github.com/PeterReid/schedule_recv

        loop {
            match ps.next().expect("no probes configured to run") {
                ProbeSchedule::Probes(probes) => {
                    let mut run_collector = collector.clone();
                    let mut shared_exec = SharedThreadProbeRunner::new();

                    for probe in probes {
                        match probe.run_mode() {
                            RunMode::SharedThread => shared_exec.push(probe)
                        }
                    }

                    for error in shared_exec.run(&mut run_collector).into_iter().filter(|r| r.is_err()).map(|r| r.unwrap_err()) {
                        error!("Probe reported an error: {}", error);
                    }
                }
                ProbeSchedule::Wait(alarms) => {
                    for event in events.map(|ce| ProbeLoopEvents::ProducerEvent(ce))
                        .merge(&alarms.map(|a| ProbeLoopEvents::Timer(a)))
                        .events() {
                        match event {
                            ProbeLoopEvents::ProducerEvent(ProducerEvent::Shutdown) => return,
                            ProbeLoopEvents::Timer(_) => break
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use sender::Collect;
    use messaging::DataValue;
    use time::Duration;
    use std::slice::Iter;
    use std::rc::Rc;

    struct StubModule<C> where C: Collect {
        name: String,
        schedule: Vec<ProbeRunPlan<C>>
    }

    struct StubProbe {
        name: String
    }

    impl StubProbe {
        fn new(name: &str) -> Rc<Self> {
            Rc::new(
                StubProbe {
                    name: name.to_string()
                }
            )
        }
    }

    impl<C> Probe<C> for StubProbe where C: Collect {
        fn name(&self) -> &str {
            &self.name
        }

        fn run(&self, collector: &mut C) -> Result<(), String> {
            let mut collector = collector;
            collector.collect("foo", &self.name, "c1", DataValue::Text(format!("{}-{}", self.name, "c1")));
            collector.collect("bar", &self.name, "c2", DataValue::Text(format!("{}-{}", self.name, "c2")));
            Ok(())
        }

        fn run_mode(&self) -> RunMode {
            RunMode::SharedThread
        }
    }

    impl<C> StubModule<C> where C: Collect  {
        fn new(name: &str) -> StubModule<C> {
            StubModule {
                name: name.to_string(),
                schedule: Vec::new()
            }
        }

        fn add_schedule(&mut self, every: Duration, probe: Rc<Probe<C>>) {
            self.schedule.push(
                ProbeRunPlan {
                    every: every,
                    probe: probe
                }
            );
        }
    }

    impl<C> Module<C> for StubModule<C> where C: Collect {
        fn name(&self) -> &str {
            &self.name
        }

        fn schedule(&self) -> Iter<ProbeRunPlan<C>> {
            self.schedule.iter()
        }
    }

    struct StubCollector {
        pub values: Vec<DataValue>
    }

    impl Collect for StubCollector {
        fn collect(&mut self, _location: &str, _path: &str, _component: &str, value: DataValue) -> () {
            self.values.push(value);
        }
    }

    impl StubCollector {
        fn text_values(self) -> Vec<String> {
            self.values.into_iter().map(|v| if let DataValue::Text(c) = v { c } else { "none".to_string() }).collect()
        }
    }

    #[test]
    fn shared_thread_probe_executor() {
        let p1 = StubProbe::new("p1");
        let p2 = StubProbe::new("p2");
        let p3 = StubProbe::new("p3");

        let mut exec = SharedThreadProbeRunner::new();
        exec.push(p1);
        exec.push(p2);
        exec.push(p3);

        let mut collector = StubCollector { values: Vec::new() };
        exec.run(&mut collector);

        assert_eq!(collector.text_values(), vec![
           "p1-c1", "p1-c2",
           "p2-c1", "p2-c2",
           "p3-c1", "p3-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_next_should_provide_porbes_according_to_schedule() {
        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(100), StubProbe::new("m2-p1"));
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p2"));


        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        let mut collector = StubCollector { values: Vec::new() };
        if let ProbeSchedule::Wait(stream) = ps.next().unwrap() {
            stream.events().next();
        } else {
            panic!("expected need to wait");
        }
        if let ProbeSchedule::Probes(probes) = ps.next().unwrap() {
            for probe in probes {
                probe.run(&mut collector).unwrap();
            }
        } else {
            panic!("expected probes");
        }

        assert_eq!(collector.text_values(), vec![
           "m1-p1-c1", "m1-p1-c2",
           "m2-p1-c1", "m2-p1-c2"
        ]);
    }

    #[test]
    fn probe_scheduler_next_should_count_overrun_schedules() {
        use std::thread::sleep;
        use std::time::Duration as StdDuration;

        let mut m1 = StubModule::new("m1");
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p1"));
        m1.add_schedule(Duration::milliseconds(100), StubProbe::new("m1-p2"));

        let mut m2 = StubModule::new("m2");
        m2.add_schedule(Duration::milliseconds(200), StubProbe::new("m2-p1"));

        let mut ps: ProbeScheduler<StubCollector> = ProbeScheduler::new();
        ps.schedule(&m1);
        ps.schedule(&m2);

        sleep(StdDuration::from_millis(200));

        {
            let result = ps.next();
            assert!(result.is_ok());

            let mut collector = StubCollector { values: Vec::new() };
            if let ProbeSchedule::Probes(probes) = result.unwrap() {
                for probe in probes {
                    probe.run(&mut collector).unwrap();
                }
            } else {
                panic!("expected probes");
            }

            assert_eq!(collector.text_values(), vec![
               "m2-p1-c1", "m2-p1-c2",
               "m1-p1-c1", "m1-p1-c2",
               "m1-p2-c1", "m1-p2-c2"
            ]);
        }

        assert_eq!(ps.overrun(), 2);
    }

    #[test]
    fn timer() {
        use super::Timer;

        let timer = Timer::spawn();
        assert!(timer.alarm_in(Duration::milliseconds(10)).events().next().is_some());
        assert!(timer.alarm_in(Duration::milliseconds(10)).events().next().is_some());
    }
}

