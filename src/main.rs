use std::net::UdpSocket;
use std::process::exit;
use std::env::temp_dir;
use clap::Parser;
use flowstats::StatsMessage;
use std::time::{Duration, SystemTime};
use flowprocessor::{FlowMessage, FlowProcessor};
use signal_hook::consts::{SIGABRT, SIGINT};
use signal_hook::iterator::Signals;
use flowcollector::FlowCollector;
use tracing::level_filters::LevelFilter;
use std::thread;
use std::sync::Once;
use crossbeam::channel::{unbounded, Sender};
use tracing::{info, debug, error};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use configparser::ini::Ini;

pub mod flowprocessor;
pub mod flowcollector;
pub mod flowstats;
pub mod flowwriter;

// This will be called when SIGINT/SIGABRT is received
// Can be tested by the main loop to see if we need to exit
static STOP: Once = Once::new();

#[derive(Debug, Default)]
pub struct CollectorConfig {
    pub port        :u16,
    pub name        :String,
}

#[derive(Debug, Default)]
pub struct MainConfig {
    pub sample_interval : u64,
    pub db_table        : Option<String>,
    pub ttl             : u64,
    pub ch_host         : Option<String>,
    pub ch_user         : Option<String>,
    pub ch_pwd          : Option<String>,
    pub threshold       : u64,
    pub datadir         : String,
    pub rotation        : u64,
    pub logdir          : Option<String>,
    pub collectors      : Vec<CollectorConfig>,
}


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Specifies onfiguration file to use
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    /// Directory where parquet files are stored
    #[arg(short, long, value_name = "OUTPUT DIRECTORY")]
    directory: Option<String>,

    /// Time between rotation of parquet output
    #[arg(short, long, value_name = "MINUTES")]
    rotation: Option<u64>,

    /// Set logging to debug level
    #[arg(long, default_value("false"))]
    debug: bool,
    
}

fn get_next_timer(minutes:u64) -> Duration {
    let secs: u64 = minutes * 60;
    let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let next_ts:Duration = Duration::new((u64::from( now_ts.as_secs() / secs)+1)*secs , 0);
    
    next_ts
}

fn parse_config(config_file: String) -> Option<MainConfig> {

    let mut cfg = MainConfig::default();
    let mut retcfg:Option<MainConfig> = None;
    let mut config = Ini::new();
    let map = config.load(config_file);
    let mut port_def = 9995;
    match map {
        Ok(items) => {
            for section in items.keys() {
                if section == "default" {
                    cfg.logdir = config.get(section, "logdir");
                    cfg.db_table = config.get(section, "db_table");
                    cfg.datadir = config.get(section, "datadir").or_else(|| {Some(temp_dir().display().to_string())}).unwrap();
                    cfg.rotation = config.getuint(section, "rotation").unwrap().or_else(|| {Some(0)}).unwrap();
                    cfg.ttl = config.getuint(section, "ttl").unwrap().or_else(||{Some(0)}).unwrap(); 
                    cfg.threshold = config.getuint(section, "threshold").unwrap().or_else(||{Some(250)}).unwrap();
                    cfg.sample_interval = config.getuint(section, "sample_interval").unwrap().or_else(||{Some(1)}).unwrap();
                    cfg.ch_host = config.get(section, "ch_host");
                    cfg.ch_user = config.get(section, "ch_user");
                    cfg.ch_pwd = config.get(section, "ch_pwd");
                } else {
                    let mut collector = CollectorConfig::default();
                    collector.name = section.clone();
                    match config.getuint(section, "port").unwrap() {
                        Some(port) => collector.port = port as u16,
                        None => {
                            collector.port = port_def;
                            port_def += 1;
                        }
                    }
                    cfg.collectors.push(collector);
                }
            }
            retcfg = Some(cfg);
        }
        Err(e) => eprintln!("Could not load config: {:?}", e),
    }

    retcfg
}

fn main() {

    let args = Args::parse();
    // println!("{:?}", args);

    // set a default config
    let mut config = MainConfig::default();
    config.datadir  = temp_dir().display().to_string();
    config.rotation = 5;
    config.threshold = 250;
    config.sample_interval = 1;

     // Read config file if provided
     match args.config {
        Some(conf_file) => {
            match parse_config(conf_file) {
                Some(cfg) => config = cfg,
                None => (),
            }
        }
        _ => {},
    }

    let (non_blocking, _guard) = match config.logdir.clone() {
        Some(logdir) => {
            let logfile = RollingFileAppender::builder()
                .rotation(Rotation::DAILY) // rotate log files once per day
                .filename_prefix("nf2pq") // log files will have names like "mywebservice.logging.2024-01-09"
                .filename_suffix("log")
                .max_log_files(7) // the number of log files to retain
                .build(logdir) // write log files to the '/var/log/mywebservice' directory
                .expect("failed to initialize rolling file appender");
            tracing_appender::non_blocking(logfile)
        }
        None => {
            tracing_appender::non_blocking(std::io::stderr())
        }
    };

    let mut level = LevelFilter::INFO;
    if args.debug {
        level = LevelFilter::DEBUG
    }

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_max_level(level)
        .init();

    info!("Starting nf2pq");

    // Override some config items if given on the command line
    match args.rotation {
        Some(rotation) => config.rotation = rotation,
        None => (),
    }

    match args.directory {
        Some(dir) => config.datadir = dir,
        None => (),
    }

    // See if we need to create a default collector
    if config.collectors.len() == 0 {
        // create a default flow collector
        let mut collector = CollectorConfig::default();
        collector.port = 9995;
        collector.name = "default".to_string();
        config.collectors.push(collector);
    }

    // Log configuration, making it easier to spot errors in it.
    info!("CONFIG\n{:#?}", config);

    // create a signal handler to handle Ctrl+C and SIGABRT
    let mut signals = Signals::new([SIGINT, SIGABRT]).unwrap();
    thread::spawn(move || {
        for sig in signals.forever() {
            match sig {
                SIGINT  => debug!("Received SIGINT"),
                SIGABRT => debug!("Received SIGABRT"),
                _       => debug!("Received signal {:?}", sig),
            }
            STOP.call_once(|| {});
        }
    });

    // Create a channel for each flow collector (from fc to fp).
    // Each fc has a channel between it and its processing thread
    // to get the UDP datagrams out of the receiving buffer ASAP.
    // The final step is a (single) flow writer thread that collects
    // the flow stats of each processor and writes then to parquet 
    // files on disk and optionally pushes those to clickhouse
    let mut fc_threads = Vec::new();
    let mut fp_txs: Vec<Sender<FlowMessage>> = Vec::new();
    let mut fp_threads = Vec::new();
    
    // message channel between processor(s) and writer
    // each processor gets a clone of the Sender
    let (fw_tx, fw_rx) = unbounded::<StatsMessage>();

    // Create all flow collectors and processors
    for flowsource in &config.collectors {
        // message channel between collector and processor
        let (fp_tx, fp_rx) = unbounded::<FlowMessage>();
        let collector_result = FlowCollector::new(
            flowsource.name.clone(), 
            flowsource.port, 
            fp_tx.clone());
        match collector_result {
            Ok(mut collector) => {
                let fc_thread = thread::spawn(move || {
                    collector.start();
                });
                fc_threads.push(fc_thread);
            }
            Err( _ ) => {
                error!("Failed to create flow collector '{}', exiting", flowsource.name.clone());
                exit(1);
            }
        }
        fp_txs.push(fp_tx);

        let processor_result = FlowProcessor::new(
            flowsource.name.clone(), 
            config.sample_interval,
            fp_rx,
            fw_tx.clone());
        match processor_result {
            Ok(mut processor) => {
                let fp_thread = thread::spawn(move || {
                    processor.start();
                    });
                fp_threads.push(fp_thread);
            }
            Err( _ ) => {
                error!("Failed to create flow processor '{}', exiting", flowsource.name.clone());
                exit(1);
            },
        }
    }

    // Now create the one flow writer
    let mut flowwriter = flowwriter::FlowWriter::new(
        fw_rx,
        config.datadir,
        config.threshold*1000,
        config.db_table,
        // None,
        config.ttl,  // TTL. 0 means no TTL
        config.ch_host,
        config.ch_user,
        config.ch_pwd,
    );

    let fw_thread = thread::spawn(move || {
        flowwriter.start();
    });

    // What is the next time we need to rotate files?
    let mut next_ts = get_next_timer(config.rotation);

    // Everything prepared. Keep on looping until we receive a signal to stop (SIGINT or SIGABRT)
    loop {
        let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        if now_ts > next_ts {
            let _ = fw_tx.send(StatsMessage::Command("tick".to_string())).unwrap();
            next_ts = get_next_timer(config.rotation);
        }
        // check if we received a SIGINT/SIGABRT signal
        if STOP.is_completed() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Send a quit command to each processor
    // This will cause them to stop and exit
    // Which the flow writer will pick up on
    // because of the closed channel
    for tx in fp_txs {
        tx.send(FlowMessage::Command("quit".to_string())).unwrap();
        drop(tx);
    }

    // wait for the processor threads to exit cleanly
    for fp_thread in fp_threads {
        match fp_thread.join() {
            Ok(_) => (),
            Err(err) => {
                error!("{:#?}", err);
            }
        }
    }
    debug!("All flow processors have stopped");

    // At this point all channels between collector(s) and
    // processor(s) have closed. Collector(s) will stop
    // when they notice this, but may wait endlessly 
    // for a UDP packet to arrive. So we send one to each
    // Since sending it to a processor will fail (and lead to exit),
    // it doesn't really matter what we send. It is just a wake-up packet
    let socket_r = UdpSocket::bind("127.0.0.1:0");
    match socket_r {
        Ok(socket) => {
            let message = String::from("quit").into_bytes();
            for collector in &config.collectors {
                let _ = socket.send_to(&message, format!("127.0.0.1:{}", collector.port));
            }
        }
        Err(e) => {
            error!("Could not create UDP socket - {:?}", e);
        }
    }

    // wait for the collector threads to exit cleanly
    for fc_thread in fc_threads {
        match fc_thread.join() {
            Ok(_) => (),
            Err(err) => {
                error!("{:#?}", err);
            }
        }
    }
    debug!("All flow collectors have stopped");


    // finally drop last reference to flowwriter channel
    // and wait for the flowwriter thread to finish
    drop(fw_tx);
    match fw_thread.join() {
        Ok(_) => (),
        Err(err) => {
            error!("{:#?}", err);
        }
    }
    debug!("Flow writer has stopped");

    // We can now exit cleanly
    info!("Exiting nf2pq");

}
