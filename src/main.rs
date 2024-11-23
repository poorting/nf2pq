use std::net::UdpSocket;
use std::process::exit;
use std::env::temp_dir;
use clap::Parser;
use flowstats::StatsMessage;
use std::time::{Duration, SystemTime};
use flowprocessor::{FlowMessage, FlowProcessor};
use signal_hook::consts::{SIGABRT, SIGINT};
use signal_hook::iterator::Signals;
use flowccollector::FlowCollector;
use tracing::level_filters::LevelFilter;
use std::thread;
use std::sync::Once;
use crossbeam::channel::{unbounded, Sender};
use tracing::{instrument, info, debug, error};
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
fn get_next_timer(minutes:u64) -> Duration {
    let secs: u64 = minutes;
    let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let next_ts:Duration = Duration::new((u64::from( now_ts.as_secs() / secs)+1)*secs , 0);
    // let next_dt:DateTime<Local> = Utc.timestamp_opt(next_ts.as_secs() as i64, 0).unwrap().into();
    // let next_dt_str = next_dt.format("%Y%m%d%H%M").to_string();
    
    next_ts
}

// #[tokio::main(flavor = "multi_thread", worker_threads = 5)]
// #[instrument]
fn main() {

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
    // The final step is a flow writer thread that collects
    // the flow stats and writes then to parquet files on disk
    // and optionally pushes those to clickhouse
    let mut fc_threads = Vec::new();
    let mut fp_txs: Vec<Sender<FlowMessage>> = Vec::new();
    let mut fp_threads = Vec::new();
    
    // Create all flow collectors and processors (start with only one pair)
    // message channel between processor(s) and writer
    let (fw_tx, fw_rx) = unbounded::<StatsMessage>();

    // 11111111111111111111111111111111111111111111111111111111111111111111111
    // message channel between collector and processor
    let (fp_tx, fp_rx) = unbounded::<FlowMessage>();
    let collector_result = FlowCollector::new(
        // "192.168.0.7".to_string(), 
        None,
        "first".to_string(), 
        9995, 
        fp_tx.clone());
    match collector_result {
        Ok(mut collector) => {
            let fc_thread = thread::spawn(move || {
                collector.start();
            });
            fc_threads.push(fc_thread);
        }
        Err( _ ) => {
            info!("Failed to create flow collector, exiting");
            exit(1);
        }
    }
    fp_txs.push(fp_tx);

    let processor_result = FlowProcessor::new(
        "first".to_string(), 
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
            info!("Failed to create flow processor, exiting");
            exit(1);
        },
    }

    // // 2222222222222222222222222222222222222222222222222222222222222222222222222
    // message channel between collector and processor
    let (fp_tx, fp_rx) = unbounded::<FlowMessage>();
    let collector_result = FlowCollector::new(
        // "192.168.0.7".to_string(), 
        None,
        "second".to_string(), 
        9996, 
        fp_tx.clone());
    match collector_result {
        Ok(mut collector) => {
            let fc_thread = thread::spawn(move || {
                collector.start();
            });
            fc_threads.push(fc_thread);
        }
        Err( _ ) => {
            info!("Failed to create flow collector, exiting");
            exit(1);
        }
    }
    fp_txs.push(fp_tx);

    let processor_result = FlowProcessor::new(
        "second".to_string(), 
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
            info!("Failed to create flow processor, exiting");
            exit(1);
        },
    }

    // -----------------------------------------------------------------------

    // Now create the one flow writer
    let mut flowwriter = flowwriter::FlowWriter::new(
        fw_rx,
        args.directory,
        Some("testdb.testflows".to_string()),
        0,  // TTL. 0 means no TTL
        None,
        None,
        None,
    );

    let fw_thread = thread::spawn(move || {
        flowwriter.start();
    });

    // What is the next time we need to rotate files?
    let mut next_ts = get_next_timer(args.rotation);
    // debug!("First rotation at {}", next_ts_str);

    // Everything prepared. Keep on looping until we receive a signal to stop (SIGINT or SIGABRT)
    loop {
        let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        // let next_ts_str:DateTime<Local> = Utc.timestamp_opt(next_ts.as_secs() as i64, 0).unwrap().into();
        if now_ts > next_ts {
            // send_cmd(9995, &format!("flush {}", next_ts_str));
            let _ = fw_tx.send(StatsMessage::Command("tick".to_string())).unwrap();
            // for tx in fc_txs.clone() {
            //     let _ = tx.send("tick".to_string());
            // }
            next_ts = get_next_timer(args.rotation);
        }
        // check if we received a SIGINT/SIGABRT signal
        if STOP.is_completed() {
            // No need to send quit command
            // dropping of channels will take care of that
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

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
    let socket_r = UdpSocket::bind("127.0.0.1:0");
    match socket_r {
        Ok(socket) => {
            let message = String::from("quit").into_bytes();
            let _ = socket.send_to(&message, "127.0.0.1:9995");
            let _ = socket.send_to(&message, "127.0.0.1:9996");
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


    // finally drop flowwriter channel and wait for the flowwriter thread to finish
    drop(fw_tx);
    match fw_thread.join() {
        Ok(_) => (),
        Err(err) => {
            error!("{:#?}", err);
        }
    }
    debug!("Flow writer has stopped");

    info!("Exit nf2ch");

}
