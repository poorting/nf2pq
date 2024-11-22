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

use crate::flowstats::*;


#[derive(Debug)]
pub struct FlowWriter {
    rx          : channel::Receiver<StatsMessage>,
    base_dir    : String,
    schema      : Schema,           // Schema of the parquet file
    writer      : ArrowWriter<std::fs::File>,
    flows       : Vec<FlowStats>,   // Will contain flowstats
    rotations   : u64,              // Number of rotations due to size between rotation ticks (timers)
                                    // Used to determine if a rotation tick should
                                    // lead to a forced rotation (shutdown will always do that)
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
        ch_db_table : Option<String>,   // DB and table to write to (if any)
        ch_ttl      : u64,              // TTL to set for the table (0 if none)
        ch_host     : Option<String>,   // CH Host to use (localhost if None)
        ch_user     : Option<String>,   // CH Username (default if None)
        ch_pwd      : Option<String>,   // CH Password (or None)
        ) -> FlowWriter
    {
        let fields = FlowStats::create_fields();
        let schema = Schema::new(fields.clone());
        let filename = format!("{}/flows.current.parquet", base_dir);
        let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::SNAPPY)
        .build();
    // .set_column_encoding(ColumnPath::from("col1"), Encoding::DELTA_BINARY_PACKED)
    
        // eprintln!("Trying to open file {}", filename);
        let file =  File::create(filename.clone()).unwrap();

        let writer = 
            ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .expect("Error opening parquet file");
   
        let mut fw = FlowWriter {
            rx          : rx,
            base_dir    : base_dir.to_string(),
            schema      : schema.clone(),
            writer      : writer,
            flows       : Vec::new(),
            rotations   : 0,
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
                        // debug!("Received rotation timer tick");
                        // if let Some(fw) = self.flow_writer.as_mut() {
                        //     fw.rotate_tick(true);
                        // }
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

    }

    fn push(&mut self, flow: FlowStats) {

        self.flows.push(flow);

        if self.flows.len() >= 250_000 {
            // self.write_batch();
            self.rotate(true);
            self.rotations += 1;
        }

    }

    fn write_batch(&mut self) {
        let batch = self.record_batch();
        debug!("Writing batch ({} flows)", batch.num_rows());
        self.writer.write(&batch).unwrap();
        self.flows = Vec::new();
    }


    fn close_current(&mut self) {
        // self.flush();
        // write any remaining flows
        self.write_batch();
        let _ = self.writer.flush();
        match self.writer.finish() {
            Err(error) => {
                error!("Error closing writer : {:?}", error);
            }
            _ => {
                debug!("Closed writer");
            }
        }
    }

    fn rotate_tick(&mut self, open_new: bool) {

        debug!("FlowWriter tick");
        if !open_new {  // Not opening a new one means we're shutting down
            // set rotations to zero, so we're sure to flush
            self.rotations = 0;
        }

        // If we have had more than 2 rotations between ticks: no need to force
        if self.rotations < 3 {
            // If we have no flows then no need to insert either
            if self.flows.len() > 0 {
                self.rotate(open_new);
            }
        } else {
            debug!("Already enough rotations, not forcing");
        }
        self.rotations = 0;
    }

    fn rotate(&mut self, open_new:bool) {
        // Close current, rename, open new
        self.close_current();
        let loc_now: DateTime<Local> = Local::now();
        debug!(" -> {}",loc_now.format("%Y-%m-%d-%H:%M:%S%.6f"));
        let filename = format!("{}/flows.current.parquet", self.base_dir.clone());
        // Rename to naming scheme
        let to_file = format!("{}/flows-{}.parquet", self.base_dir.clone(), loc_now.format("%Y-%m-%d-%H:%M:%S%.6f"));
        debug!("rename {} -> {}", filename.clone(), to_file.clone());
        std::fs::rename(filename.clone(), to_file.clone()).unwrap();
        // Create new one if requested
        if open_new {
            let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::SNAPPY)
            .build();
        // .set_column_encoding(ColumnPath::from("col1"), Encoding::DELTA_BINARY_PACKED)
        
            // eprintln!("Trying to open file {}", filename);
            let file = File::create(filename).unwrap();
            self.writer = ArrowWriter::try_new(file, Arc::new(self.schema.clone()), Some(props)).unwrap();
        }
    
        // If we have a database then insert it into database and remove the rotated file
        if self.insert_ch {
            // Setting stdin to this file does not always work for some reason
            // No flows inserted, but no errors either
            // Following query (letting client read file itself) does seem to work however.
            let query = format!("INSERT INTO {} FROM INFILE '{}' FORMAT Parquet", 
                self.ch_db_table.as_ref().clone().unwrap(),
                to_file.clone());
            self.ch_query(query);

            // If we insert into CH, then we delete the file afterwards
            std::fs::remove_file(to_file.clone()).unwrap();

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
        // let icmp_type = UInt8Array::from(self.flows.iter().map(|p| p.icmp_type).collect::<Vec<Option<u8>>>());
        // let icmp_code = UInt8Array::from(self.flows.iter().map(|p| p.icmp_code).collect::<Vec<Option<u8>>>());
        let ipkt = UInt64Array::from(self.flows.iter().map(|p| p.ipkt).collect::<Vec<Option<u64>>>());
        let ibyt = UInt64Array::from(self.flows.iter().map(|p| p.ibyt).collect::<Vec<Option<u64>>>());
        let smk = UInt8Array::from(self.flows.iter().map(|p| p.smk).collect::<Vec<Option<u8>>>());
        let dmk = UInt8Array::from(self.flows.iter().map(|p| p.dmk).collect::<Vec<Option<u8>>>());
        let ra = GenericStringArray::<i32>::from(self.flows.iter().map(|p| p.ra.clone()).collect::<Vec<Option<String>>>());
        let inif = UInt16Array::from(self.flows.iter().map(|p| p.inif).collect::<Vec<Option<u16>>>());
        let outif = UInt16Array::from(self.flows.iter().map(|p| p.outif).collect::<Vec<Option<u16>>>());
        let sas = UInt32Array::from(self.flows.iter().map(|p| p.sas).collect::<Vec<Option<u32>>>());
        let das = UInt32Array::from(self.flows.iter().map(|p| p.das).collect::<Vec<Option<u32>>>());
        let exid = UInt16Array::from(self.flows.iter().map(|p| p.exid).collect::<Vec<Option<u16>>>());
        // let flowsrc = GenericStringArray::<i32>::from(vec![self.source_name.clone(); self.flows.len()]);
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
                // Arc::new(icmp_type),
                // Arc::new(icmp_code),
                Arc::new(ipkt),
                Arc::new(ibyt),
                Arc::new(smk),
                Arc::new(dmk),
                Arc::new(ra),
                Arc::new(inif),
                Arc::new(outif),
                Arc::new(sas),
                Arc::new(das),
                Arc::new(exid),
                Arc::new(flowsrc),
                ]
        ).unwrap();

        return batch;

    }

    // fn ch_insert(&mut self, filename: String) {
    //     debug!("CH insert: {}", filename.clone());

    //     let query = format!("INSERT INTO {} FORMAT Parquet", self.ch_db_table.as_ref().clone().unwrap());

    //     let mut cmd = Command::new("clickhouse-client");
    //     // if self.username.is_some() ...
    //     if self.ch_host.is_some() {
    //         cmd.arg("--host");
    //         cmd.arg(self.ch_host.as_ref().unwrap().clone());
    //     }
    //     if self.ch_user.is_some() {
    //         cmd.arg("--user");
    //         cmd.arg(self.ch_user.as_ref().unwrap().clone());
    //     }
    //     if self.ch_pwd.is_some() {
    //         cmd.arg("--password");
    //         cmd.arg(self.ch_pwd.as_ref().unwrap().clone());
    //     }
    //     cmd.arg("--query");
    //     cmd.arg(query);
    // }

    fn ch_query(&mut self, query: String) {

        debug!("CH query: {}", query.clone());

        let mut cmd = Command::new("clickhouse-client");
        // if self.username.is_some() ...
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
    

        // let output = match cmd.output() {
        let _output = match cmd.spawn().expect("Error insert").wait_with_output() {
            Err(e) => debug!("{:?}", e),
            Ok(_output) => {
                debug!("OK");
                // if output.status.success() {
                //     debug!("stdout: {:?}", String::from_utf8(output.stdout) );
                //     debug!("stderr: {:?}", String::from_utf8(output.stderr) );
                // } else {
                //     debug!("Process failed: {:?}", String::from_utf8(output.stderr) );
                // }
            }
        };
        // debug!("{:#?}", output);
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
                `ipkt` UInt64,
                `ibyt` UInt64,
                `smk` UInt8,
                `dmk` UInt8,
                `ra` LowCardinality(String),
                `inif` UInt16 DEFAULT 0,
                `outif` UInt16 DEFAULT 0,
                `sas` UInt32 DEFAULT 0,
                `das` UInt32 DEFAULT 0,
                `exid` UInt16 DEFAULT 0,
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