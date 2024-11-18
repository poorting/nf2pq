use std::sync::Arc;
use std::fs::File;
use arrow::datatypes::*;
use arrow::array::*;
use parquet::{
    basic::{Compression, Encoding},
    file::properties::*,
    arrow::ArrowWriter,
};
use tracing::{info, debug, error};
use clickhouse::{Client, inserter, Row};

use crate::flowstats::*;

// #[derive(Debug)]
pub struct FlowInserter {
    db_and_table: String,
    ttl         : u64,
    client      : clickhouse::Client,
    // inserter    : clickhouse::inserter::Inserter<FlowStats>
    flows       : Vec<FlowStats>,
}


impl FlowInserter {
    pub fn new(
        db_and_table: String,
        ttl: u64,
    ) -> Result<FlowInserter, std::io::Error>
    {
        let client = clickhouse::Client::default()
            .with_url("http://localhost:8123")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");
            // let inserter = client.inserter(&db_and_table).unwrap();
        let mut fi = FlowInserter {
            db_and_table: db_and_table.clone(),
            ttl,
            client: client,
            // inserter: inserter,
            flows: Vec::new(),
        };
        // async { let _ = fi.create_db().await; };
        // let _ = executor::block_on(fi.create_db());
        return Ok(fi);
    }

    pub fn create_db_and_table(&mut self) {
        // let query = format!("CREATE DATABASE IF NOT EXISTS {};",self.db_and_table.clone().split(".").nth(0).unwrap());
        // let result = self.client.query(&query).execute().await;
        // debug!("CREATE DB: {:?}", result);

        // let query = format!("
        //     CREATE TABLE IF NOT EXISTS {}
        //     (
        //         `ts` DateTime64(6) DEFAULT 0,
        //         `te` DateTime64(6) DEFAULT 0,
        //         `sa` String,
        //         `da` String,
        //         `sp` UInt16 DEFAULT 0,
        //         `dp` UInt16 DEFAULT 0,
        //         `pr` Nullable(String),
        //         `flg` LowCardinality(String),
        //         `ipkt` UInt64,
        //         `ibyt` UInt64,
        //         `smk` UInt8,
        //         `dmk` UInt8,
        //         `ra` LowCardinality(String),
        //         `inif` UInt16 DEFAULT 0,
        //         `outif` UInt16 DEFAULT 0,
        //         `sas` UInt32 DEFAULT 0,
        //         `das` UInt32 DEFAULT 0,
        //         `exid` UInt16 DEFAULT 0,
        //         `flowsrc` LowCardinality(String)
        //     )
        //     ENGINE = MergeTree
        //     PARTITION BY tuple()
        //     PRIMARY KEY (ts, te)
        //     ORDER BY (ts, te, sa, da)"
        //     ,self.db_and_table.clone()
        // );
        // //             TTL te + toIntervalDay({})"

        // let result = self.client.query(&query).execute().await;
        // debug!("CREATE TABLE: {:?}", result);
        // // self.client.in

    }

    pub fn push(&mut self, flow: FlowStats) {
        // push flowstat to clickhouse
        // let mut insert = self.client.insert(&self.db_and_table).unwrap();
        // let result = insert.write(&flow).await;
        // info!("insert: {:?}", result);
        // let result = insert.end().await;
        // info!("insert end: {:?}", result);
        // // let query = format!("INSERT INTO {} SETTINGS async_insert=1, wait_for_async_insert=0 VALUES ({},{},'{}','{}',{},{},'{}','{}',{},{},{},{},'{}',{},{},{},{},{},'{}');",
        // //     self.db_and_table.clone(), flow.ts.unwrap(), flow.te.unwrap(), flow.sa.unwrap(), flow.da.unwrap(), flow.sp.unwrap_or(0), flow.dp.unwrap_or(0), 
        // //     flow.pr.unwrap(), flow.flg.unwrap(), flow.ipkt.unwrap(), flow.ibyt.unwrap(), flow.smk.unwrap_or(0),flow.dmk.unwrap_or(0), 
        // //     flow.ra.unwrap_or("".to_string()), flow.inif.unwrap_or(0),flow.outif.unwrap_or(0), flow.sas.unwrap_or(0), flow.das.unwrap_or(0), 
        // //     flow.exid.unwrap_or(0), flow.flowsrc.unwrap());
        // // // info!("{}", query.clone());
        // // let result = self.client.query(&query).execute().await;
        // // info!("INSERT: {:?}", result);

    }
}
