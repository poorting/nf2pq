use std::sync::Arc;
use std::fs::File;
use arrow::datatypes::*;
use arrow::array::*;
use crossbeam::channel;
use parquet::{
    basic::{Compression, Encoding},
    file::properties::*,
    arrow::ArrowWriter,
};
use chrono::*;
use tracing::{info, debug, error};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::flowstats::*;


#[derive(Debug)]
pub struct FlowWriter {
    rx          : channel::Receiver<StatsMessage>,
    base_dir    : String,
    threshold   : u64,              // Number of flows collected that will lead to a rotation
    schema      : Schema,           // Schema of the parquet file
    flows       : Vec<FlowStats>,   // Will contain flowstats
    last_rot    : Instant,          // Set when rotated, helps to determine if flush needed at next tick 
    insert_ch   : bool,             // Will be set to true if a db_table is given
    ch_db_table : Option<String>,   // DB and table to write to (if any)
    ch_ttl      : u64,              // TTL to set for the table (0 if none)
    ch_host     : Option<String>,   // CH Host to use (localhost if None)
    ch_user     : Option<String>,   // CH Username (default if None)
    ch_pwd      : Option<String>,   // CH Password (or None)
}


impl FlowWriter {
    pub fn new(
        rx          : channel::Receiver<StatsMessage>,
        base_dir    : String,
        threshold   : u64,
        ch_db_table : Option<String>,   // DB and table to write to (if any)
        ch_ttl      : u64,              // TTL to set for the table (0 if none)
        ch_host     : Option<String>,   // CH Host to use (localhost if None)
        ch_user     : Option<String>,   // CH Username (default if None)
        ch_pwd      : Option<String>,   // CH Password (or None)
        ) -> FlowWriter
    {
        let fields = FlowStats::create_fields();
        let schema = Schema::new(fields.clone());
   
        let mut fw = FlowWriter {
            rx          : rx,
            base_dir    : base_dir.to_string(),
            threshold   : threshold,
            schema      : schema.clone(),
            flows       : Vec::new(),
            last_rot    : Instant::now(),
            insert_ch   : ch_db_table.is_some(),
            ch_db_table : ch_db_table,
            ch_ttl      : ch_ttl,
            ch_host     : ch_host,
            ch_user     : ch_user,
            ch_pwd      : ch_pwd,
        };

        if fw.insert_ch {
            fw.create_db_and_table();
        }

        return fw;
    }

    pub fn start(&mut self) {
                // Listen to rx, 
        // check each message if it contains command or received datagram
        for msg in self.rx.clone().iter() {
            match msg {
                StatsMessage::Command(cmd) => {
                    if cmd.starts_with("tick") {
                        self.rotate_tick(true);
                    } else {
                        println!("received command: {}", cmd);
                    }
                }
                StatsMessage::Stats(flow) => {
                    self.push(flow);
                }
            }
        }

        info!("flowwriter '{}' exiting gracefully", self.base_dir.clone());
        self.rotate_tick(false);

    }

    fn push(&mut self, flow: FlowStats) {

        self.flows.push(flow);

        if self.flows.len() as u64 >= self.threshold {
            let delta: f64 = self.last_rot.elapsed().as_millis() as f64;
            debug!("Buffer threshold reached. Rotating. {:.1} kflows/s", self.threshold as f64/delta);
            // self.write_batch();
            self.rotate();
        }

    }

    fn rotate_tick(&mut self, open_new: bool) {

        debug!("flowwriter tick ({:?})", self.last_rot.elapsed() );

        if self.last_rot.elapsed() > Duration::from_secs(2) || !open_new {
            if self.flows.len() > 0 {
                self.rotate();
            }
        } else {
            debug!("Last rotation less than 2 seconds ago, not bothered");
        }
    }

    fn rotate(&mut self) {
        // reset timer
        self.last_rot = Instant::now();
        // Close current, rename, open new
        // self.close_current();
        let loc_now: DateTime<Local> = Local::now();
        // let filename = format!("{}/flows.current", self.base_dir.clone());
        // Rename to naming scheme
        let to_file = format!("{}/flows-{}.parquet", self.base_dir.clone(), loc_now.format("%Y-%m-%d-%H:%M:%S%.6f"));
        // std::fs::rename(filename.clone(), to_file.clone()).unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::SNAPPY)
            .build();
        // eprintln!("Trying to open file {}", filename);
        let file = File::create(to_file.clone()).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::new(self.schema.clone()), Some(props)).unwrap();
    
        let batch = self.record_batch();
        debug!("Writing {} flows to {}", batch.num_rows(), to_file.clone());
        writer.write(&batch).unwrap();
        let _ = writer.flush();
        match writer.finish() {
            Err(error) => {
                error!("Error closing writer : {:?}", error);
            }
            _ => (),
        }

        self.flows = Vec::new();

        
        // If we have a database then insert it into database and remove the rotated file
        if self.insert_ch {
            // Setting stdin to this file does not always work for some reason
            // No flows inserted, but no errors either
            // Following query (letting client read file itself) does seem to work however.
            let query = format!("INSERT INTO {} FROM INFILE '{}' FORMAT Parquet", 
                self.ch_db_table.as_ref().clone().unwrap(),
                to_file.clone());
            if self.ch_query(query) {
                // If we insert into CH succesfully, then we delete the file afterwards
                std::fs::remove_file(to_file.clone()).unwrap();
            }
        }
    }

    fn record_batch(&mut self) -> RecordBatch {
        let ts = TimestampMicrosecondArray::from(self.flows.iter().map(|p| p.ts).collect::<Vec<Option<i64>>>());
        let te = TimestampMicrosecondArray::from(self.flows.iter().map(|p| p.te).collect::<Vec<Option<i64>>>());
        let sa = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.sa.clone()).collect::<Vec<Option<String>>>());
        let da = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.da.clone()).collect::<Vec<Option<String>>>());
        let sp = UInt16Array::from(self.flows.iter().map(|p| p.sp).collect::<Vec<Option<u16>>>());
        let dp = UInt16Array::from(self.flows.iter().map(|p| p.dp).collect::<Vec<Option<u16>>>());
        let pr = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.pr.clone()).collect::<Vec<Option<String>>>());
        let flg = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.flg.clone()).collect::<Vec<Option<String>>>());
        let icmp_type = UInt8Array::from(self.flows.iter().map(|p| p.icmp_type).collect::<Vec<Option<u8>>>());
        let icmp_code = UInt8Array::from(self.flows.iter().map(|p| p.icmp_code).collect::<Vec<Option<u8>>>());
        let pkt = UInt64Array::from(self.flows.iter().map(|p| p.pkt).collect::<Vec<Option<u64>>>());
        let byt = UInt64Array::from(self.flows.iter().map(|p| p.byt).collect::<Vec<Option<u64>>>());
        let smk = UInt8Array::from(self.flows.iter().map(|p| p.smk).collect::<Vec<Option<u8>>>());
        let dmk = UInt8Array::from(self.flows.iter().map(|p| p.dmk).collect::<Vec<Option<u8>>>());
        let ra = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.ra.clone()).collect::<Vec<Option<String>>>());
        let nh = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.nh.clone()).collect::<Vec<Option<String>>>());
        let inif = UInt16Array::from(self.flows.iter().map(|p| p.inif).collect::<Vec<Option<u16>>>());
        let outif = UInt16Array::from(self.flows.iter().map(|p| p.outif).collect::<Vec<Option<u16>>>());
        let sas = UInt32Array::from(self.flows.iter().map(|p| p.sas).collect::<Vec<Option<u32>>>());
        let das = UInt32Array::from(self.flows.iter().map(|p| p.das).collect::<Vec<Option<u32>>>());
        // let exid = UInt16Array::from(self.flows.iter().map(|p| p.exid).collect::<Vec<Option<u16>>>());
        let flowsrc = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.flowsrc.clone()).collect::<Vec<Option<String>>>());

        let batch = RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(ts),
                Arc::new(te),
                Arc::new(sa),
                Arc::new(da),
                Arc::new(sp),
                Arc::new(dp),
                Arc::new(pr),
                Arc::new(flg),
                Arc::new(icmp_type),
                Arc::new(icmp_code),
                Arc::new(pkt),
                Arc::new(byt),
                Arc::new(smk),
                Arc::new(dmk),
                Arc::new(ra),
                Arc::new(nh),
                Arc::new(inif),
                Arc::new(outif),
                Arc::new(sas),
                Arc::new(das),
                // Arc::new(exid),
                Arc::new(flowsrc),
                ]
        ).unwrap();

        return batch;

    }

    fn ch_query(&mut self, query: String) -> bool {

        debug!("CH query: {}", query.clone());

        // Use 'clickhouse client' rather then 'clickhouse-client' (e.g. for Mac)
        let mut cmd = Command::new("clickhouse");
        cmd.arg("client");
        if self.ch_host.is_some() {
            cmd.arg("--host");
            cmd.arg(self.ch_host.as_ref().unwrap().clone());
        }
        if self.ch_user.is_some() {
            cmd.arg("--user");
            cmd.arg(self.ch_user.as_ref().unwrap().clone());
        }
        if self.ch_pwd.is_some() {
            cmd.arg("--password");
            cmd.arg(self.ch_pwd.as_ref().unwrap().clone());
        }
        cmd.arg("--query");
        cmd.arg(query);
    
        cmd.stderr(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::null());
        let mut success = true;
        let start = Instant::now();
        match cmd.spawn().expect("Error query").wait_with_output() {
            Err(e) => {
                success = false;
                debug!("{:?}", e)
            }
            Ok(output) => {
                // debug!("OK -> {:?}", output.status.code().unwrap());
                if output.status.code() != Some(0) {
                    success = false;
                    debug!("Some error when querying (exit code: {}, stderr={:?}", 
                        output.status.code().unwrap(), 
                        String::from_utf8(output.stderr));
                } else {
                    debug!("OK ({:?})", start.elapsed());
                }
            }
        };
        success
    }

    fn create_db_and_table(&mut self) {

        let ttlstr = match self.ch_ttl {
            0 => "",
            _ => &format!("TTL toDateTime(te) + toIntervalDay({})", self.ch_ttl)
        };

        let query = format!("
            CREATE DATABASE IF NOT EXISTS {};
            CREATE TABLE IF NOT EXISTS {}
            (
                `ts` DateTime64(6) DEFAULT 0,
                `te` DateTime64(6) DEFAULT 0,
                `sa` String,
                `da` String,
                `sp` UInt16 DEFAULT 0,
                `dp` UInt16 DEFAULT 0,
                `pr` Nullable(String),
                `flg` LowCardinality(String),
                `icmp_type` UInt8 DEFAULT 0,
                `icmp_code` UInt8 DEFAULT 0,
                `pkt` UInt64,
                `byt` UInt64,
                `smk` UInt8,
                `dmk` UInt8,
                `ra` LowCardinality(String),
                `nh` String,
                `in` UInt16 DEFAULT 0,
                `out` UInt16 DEFAULT 0,
                `sas` UInt32 DEFAULT 0,
                `das` UInt32 DEFAULT 0,
                `flowsrc` LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY tuple()
            PRIMARY KEY (ts, te)
            ORDER BY (ts, te, sa, da)
            {};",
            self.ch_db_table.as_ref().unwrap().split(".").nth(0).unwrap(),
            self.ch_db_table.as_ref().unwrap(),
            ttlstr,
        );

        self.ch_query(query);

    }

}