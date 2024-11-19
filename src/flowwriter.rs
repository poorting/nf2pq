use std::sync::Arc;
use std::fs::File;
use arrow::datatypes::*;
use arrow::array::*;
use parquet::{
    basic::{Compression, Encoding},
    file::properties::*,
    arrow::ArrowWriter,
};
use chrono::*;
use tracing::{debug, error};


use crate::flowstats::*;


#[derive(Debug)]
pub struct FlowWriter {
    source_name : String, 
    base_dir    : String,
    schema      : Schema,         // Schema of the parquet file
    writer      : ArrowWriter<std::fs::File>,
    flows       : Vec<FlowStats>,  // Will contain flowstats
}


impl FlowWriter {
    pub fn new(
        source_name: String, 
        base_dir: String,
    ) -> Result<FlowWriter, std::io::Error>
    {
        let fields = FlowStats::create_fields();
        let schema = Schema::new(fields.clone());
        let filename = format!("{}/{}.current.parquet", base_dir, source_name);
        let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::SNAPPY)
        .build();
    // .set_column_encoding(ColumnPath::from("col1"), Encoding::DELTA_BINARY_PACKED)
    
        // eprintln!("Trying to open file {}", filename);
        let file =  match File::create(filename.clone()) {
            Ok(file) => file,
            Err(error) =>
            {
                error!("Could not open file: {}", filename.clone());
                return Err(error);
            }
        };

        let writer = 
            ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .expect("Error opening parquet file");

        let fw = FlowWriter {
            source_name : source_name.to_string(),
            base_dir    : base_dir.to_string(),
            schema      : schema.clone(),
            writer      : writer,
            flows       : Vec::new(),
        };
        return Ok(fw);
    }

    pub fn push(&mut self, flow: FlowStats) {

        self.flows.push(flow);

        if self.flows.len() >= 250_000 {
            // self.write_batch();
            self.rotate(true);
        }

    }

    fn write_batch(&mut self) {
        let batch = self.record_batch();
        debug!("Writing batch ('{}' - {} flows)", self.source_name, batch.num_rows());
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
                error!("Error closing writer for '{}' - {:?}", self.source_name, error);
            }
            _ => {
                debug!("Closed writer for '{}'", self.source_name);
            }
        }
    }

    pub fn rotate(&mut self, open_new:bool) {
        // Close current, rename, open new
        self.close_current();
        let loc_now: DateTime<Local> = Local::now();
        debug!(" -> {}",loc_now.format("%Y-%m-%d %H:%M:%S%.6f"));
        let filename = format!("{}/{}.current.parquet", self.base_dir.clone(), self.source_name.clone());
        // Rename to naming scheme
        let to_file = format!("{}/{}-{}.parquet", self.base_dir.clone(), self.source_name.clone(), loc_now.format("%Y-%m-%d %H:%M:%S%.6f"));
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

}