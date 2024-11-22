use std::process::exit;
use std::env::temp_dir;
use clap::Parser;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use std::time::{Duration, SystemTime};
use flowprocessor::{FlowMessage, FlowProcessor};
use signal_hook::consts::{SIGABRT, SIGINT};
use signal_hook::iterator::Signals;
use chrono::*;
use flowccollector::FlowCollector;
use tracing::level_filters::LevelFilter;
use std::thread;
use std::sync::Once;
use crossbeam::channel::unbounded;
use tracing::{instrument, info, debug};
use configparser::ini::Ini;

pub mod flowprocessor;
pub mod flowccollector;
pub mod flowstats;
pub mod flowwriter;
// pub mod flowinserter;

// This will be called when SIGINT/SIGABRT is received
// Can be tested by the main loop to see if we need to exit
static STOP: Once = Once::new();

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Specifies onfiguration file to use
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    /// Directory where parquet files are stored
    #[arg(short, long, value_name = "OUTPUT DIRECTORY", default_value_t=temp_dir().display().to_string() )]
    directory: String,

    /// Time between rotation of parquet output
    #[arg(short, long, value_name = "MINUTES", default_value("5"))]
    rotation: u64,

    /// Set logging to debug level
    #[arg(long, default_value("false"))]
    debug: bool,
    
}

#[instrument]
fn get_next_timer(minutes:u64) -> (Duration, String) {
    let secs: u64 = minutes;
    let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let next_ts:Duration = Duration::new((u64::from( now_ts.as_secs() / secs)+1)*secs , 0);
    let next_dt:DateTime<Local> = Utc.timestamp_opt(next_ts.as_secs() as i64, 0).unwrap().into();
    let next_dt_str = next_dt.format("%Y%m%d%H%M").to_string();
    
    (next_ts, next_dt_str)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
#[instrument]
async fn main() {

    let args = Args::parse();

    println!("arguments: {:?}", args);
    println!("temp dir: {}", temp_dir().display().to_string() );

    let logfile = std::io::stderr();
    // let logfile = RollingFileAppender::builder()
    //     .rotation(Rotation::DAILY) // rotate log files once per day
    //     .filename_prefix("nf2ch") // log files will have names like "mywebservice.logging.2024-01-09"
    //     .filename_suffix("log")
    //     .max_log_files(7) // the number of log files to retain
    //     .build(".") // write log files to the '/var/log/mywebservice' directory
    //     .expect("failed to initialize rolling file appender");

    let mut level = LevelFilter::INFO;
    if args.debug {
        level = LevelFilter::DEBUG
    }
    let (non_blocking, _guard) = tracing_appender::non_blocking(logfile);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_max_level(level)
        .init();

    // Read config file if provided
    match args.config {
        Some(conf_file) => {
            let mut config = Ini::new();
            let map = config.load(conf_file);
            println!("{:?}", map.clone());
            match map {
                Ok(items) => {
                    for section in items.keys() {
                        println!("section: {:?}", section);
                        let secitems = items.get(section).unwrap();
                        for item in secitems.keys() {
                            println!("\t{}={}", item, config.get(section, item).unwrap());
                        }
                    }
                }
                _ => (),
            }
        }
        _ => (),
    }
    // createw signal handler to handle Ctrl+C and SIGABRT
    let mut signals = Signals::new([SIGINT, SIGABRT]).unwrap();
    thread::spawn(move || {
        for sig in signals.forever() {
            match sig {
                SIGINT  => info!("Received SIGINT"),
                SIGABRT => info!("Received SIGABRT"),
                _       => info!("Received signal {:?}", sig),
            }
            STOP.call_once(|| {});
        }
    });

    // Create a channel for each flow collector (from main to fc).
    // Each fc creates a channel between it and the processing thread
    // to get the UDP datagrams out of the receiving buffer ASAP.
    let mut fc_threads = Vec::new();
    let mut fp_threads = Vec::new();
    let mut fc_txs: Vec<UnboundedSender<String>> = Vec::new();
    
    // Create all flow collectors and processors (start with only one pair)
    // message channel between main and collector
    let (tx, rx) = unbounded_channel::<String>();
    // message channel between collector and processor
    let (fp_tx, fp_rx) = unbounded::<FlowMessage>();

    let fc_thread = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();
        let mut collector = FlowCollector::new(
            // "192.168.0.7".to_string(), 
            None,
            "home".to_string(), 
            9995, 
            rx,
            fp_tx ).unwrap();

            rt.block_on( async {collector.start().await;});
    });
    fc_threads.push(fc_thread);
    fc_txs.push(tx);

    let flowwriter = flowwriter::FlowWriter::new(
        "home".to_string(),
        args.directory,
        Some("testdb.testflows".to_string()),
        92,
        None,
        None,
        None,
    );

    let fw = match flowwriter {
        Ok(flowwriter) => Some(flowwriter),
        _ => None,
    };

    let processor_result = FlowProcessor::new(
        "home".to_string(), 
        fp_rx,
        fw);
    match processor_result {
        Ok(mut processor) => {
            let fp_thread = thread::spawn(move || {
                processor.start();
                });
            fp_threads.push(fp_thread);
        }
        Err( _ ) => {
            info!("Exiting due to error(s)");
            exit(1);
        },
    }


    // What is the next time we need to rotate files?
    let (mut next_ts, mut next_ts_str) = get_next_timer(args.rotation);
    debug!("First rotation at {}", next_ts_str);

    // Everything prepared. Keep on looping until we receive a signal to stop (SIGINT or SIGABRT)
    loop {
        let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        // let next_ts_str:DateTime<Local> = Utc.timestamp_opt(next_ts.as_secs() as i64, 0).unwrap().into();
        if now_ts > next_ts {
            // send_cmd(9995, &format!("flush {}", next_ts_str));
            for tx in fc_txs.clone() {
                let _ = tx.send(format!("flush {}", next_ts_str));
            }
            (next_ts, next_ts_str) = get_next_timer(args.rotation);
        }
        // check if we received a SIGINT/SIGABRT signal
        if STOP.is_completed() {
            // No need to send quit command
            // dropping of channels will take care of that
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    for tx in fc_txs {
        drop(tx);
    }

    // wait for the collector threads to exit cleanly
    for fc_thread in fc_threads {
        match fc_thread.join() {
            Ok(_) => (),
            Err(err) => {
                eprintln!("{:#?}", err);
            }
        }
    }
    debug!("All flow collectors have stopped");

    // wait for the processor threads to exit cleanly
    for fp_thread in fp_threads {
        match fp_thread.join() {
            Ok(_) => (),
            Err(err) => {
                eprintln!("{:#?}", err);
            }
        }
    }
    debug!("All flow processors have stopped");

    info!("Exit nf2ch");

}
