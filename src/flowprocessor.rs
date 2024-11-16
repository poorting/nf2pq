use std::fmt::Debug;
use std::sync::Arc;
use anyhow::Error;
// use std::net::*;
// use domain::base::*;
use std::fs::File;
use bytes::BytesMut;
// use log::debug;
// use serde_json::Value;
use arrow::datatypes::*;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use netgauze_flow_pkt::netflow::NetFlowV9Packet;
use tokio_util::codec::Decoder;
use parquet::{
    basic::{Compression, Encoding},
    file::properties::*,
    arrow::ArrowWriter,
};
use netgauze_flow_pkt::{
    codec::FlowInfoCodec, 
    netflow::Set,
    FlowInfo::NetFlowV9,
    ie::Field::*,
    ie::protocolIdentifier,
};
use serde::{Deserialize, Serialize};
// use serde_json::Result;
use crossbeam::channel::{self};
use tracing::{instrument, info, debug, error};
use clickhouse::{Client, inserter, Row};


// Exchange of information between collector and processor
// can contain message (e.g. rotate flow file) or received datagram
#[derive(Debug, Clone)]
pub enum FlowMessage {
    Command(String),
    Datagram(Vec<u8>),
}


#[derive(Debug)]
pub struct FlowProcessor {
    source_name : String, 
    base_dir    : String,
    rx          : channel::Receiver<FlowMessage>,
    schema      : Schema,         // Schema of the parquet file
    writer      : ArrowWriter<std::fs::File>,
    parser      : FlowInfoCodec,
    flows       : Vec<FlowStats>,  // Will contain flowstats
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Row)]
pub struct FlowStats {
    pub ts      : Option<i64>,
    pub te      : Option<i64>,
    pub sa      : Option<String>,
    pub da      : Option<String>,
    pub sp      : Option<u16>,
    pub dp      : Option<u16>,
    pub pr      : Option<String>,
    pub flg     : Option<String>,
    // pub icmp_type: Option<u8>,
    // pub icmp_code: Option<u8>,
    pub ipkt    : Option<u64>,
    pub ibyt    : Option<u64>,
    pub smk     : Option<u8>,
    pub dmk     : Option<u8>,
    pub ra      : Option<String>,
    pub inif    : Option<u16>,
    pub outif   : Option<u16>,
    pub sas     : Option<u32>,
    pub das     : Option<u32>,
    pub exid    : Option<u16>,
}

impl FlowProcessor {
    pub fn new(
        source_name: String, 
        base_dir: String,
        rx: channel::Receiver<FlowMessage>,
    ) -> Result<FlowProcessor, std::io::Error> {

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

        let fp = FlowProcessor {
            source_name : source_name.to_string(),
            base_dir    : base_dir.to_string(),
            rx          : rx,
            schema      : schema.clone(),
            writer      : writer,
            parser      : FlowInfoCodec::default(),
            flows       : Vec::new(),
        };

        return Ok(fp);
    }

    pub fn start(&mut self) {
        // Listen to rx, 
        // check each message if it contains command or received datagram
        for msg in self.rx.clone().iter() {
            match msg {
                FlowMessage::Command(cmd) => {
                    if cmd.starts_with("flush") {
                        debug!("Received command: {}", cmd.clone());
                        let mut parts = cmd.split_whitespace();
                        if parts.clone().count()>1 {
                            debug!("Have info on {} flows", self.flows.len());
                            self.rotate(parts.nth(1).unwrap());
                        }
                    } else {
                        println!("received command: {}", cmd);
                    }
                }
                FlowMessage::Datagram(udp) => {
                    let mut bm_buf = BytesMut::with_capacity(0);
                    bm_buf.extend_from_slice(&udp);
            
                    let result = self.parser.decode(&mut bm_buf);
            
                    match result {
                        Ok(Some(pkt)) => {
                            match pkt {
                                NetFlowV9(v9pkt) => {
                                    self.process_v9packet(v9pkt);
                                }
                                _ => () // ignore everything else (IPFIX)
                            }
                        }
                        Ok(None) => {
                            debug!("Ok(None) from parser.decode");
                        },
                        Err(error) => {
                            error!("Error decoding flow packet: {:?}",error);
                        }
                    }
                }
            }
        }

        info!("flowprocessor '{}' exiting gracefully", self.source_name);
        self.close_current();

    }

    fn process_v9packet(&mut self, v9pkt: NetFlowV9Packet) {
        for set in v9pkt.sets() {
            // println!("{:?}", set);
            match set {
                Set::Data{ records, ..} => {
                    for record in records {
                        let mut flow = FlowStats::new();
                        // println!("*********************************************");
                        for field in record.fields() {
                            // println!("{:?}", field);
                            match field {
                                flowStartMilliseconds(time_ms) => flow.ts = Some(time_ms.0.timestamp_micros()),
                                flowEndMilliseconds(time_ms) => flow.te = Some(time_ms.0.timestamp_micros()),
                                sourceIPv4Address(addr) => flow.sa = Some(addr.0.to_string()),
                                destinationIPv4Address(addr) => flow.da = Some(addr.0.to_string()),
                                sourceIPv6Address(addr) => flow.sa = Some(addr.0.to_string()),
                                destinationIPv6Address(addr) => flow.da = Some(addr.0.to_string()),
                                sourceTransportPort(port) => flow.sp = Some(port.0),
                                destinationTransportPort(port) => flow.dp = Some(port.0),
                                packetDeltaCount(cnt) =>  flow.ipkt = Some(cnt.0),
                                octetDeltaCount(cnt) => flow.ibyt = Some(cnt.0),
                                protocolIdentifier(proto) => {
                                    match proto {
                                        protocolIdentifier::Unassigned(proto_nr) => flow.pr = Some(proto_nr.to_string()),
                                        _ => flow.pr = Some(proto.to_string()),
                                    }
                                }
                                tcpControlBits(flags) => flow.flg = Some(self.tcp_flags_as_string(flags.to_owned().into())),
                                sourceIPv4PrefixLength(mask) => flow.smk = Some(mask.0),
                                destinationIPv4PrefixLength(mask) => flow.dmk = Some(mask.0),
                                sourceIPv6PrefixLength(mask) => flow.smk = Some(mask.0),
                                destinationIPv6PrefixLength(mask) => flow.dmk = Some(mask.0),
                                bgpSourceAsNumber(asnr) => flow.sas = Some(asnr.0),
                                bgpDestinationAsNumber(asnr) => flow.das = Some(asnr.0),
                                ingressInterface(intf) => flow.inif = Some(intf.0 as u16),
                                egressInterface(intf) => flow.outif = Some(intf.0 as u16),

                                ipv4RouterSc(addr) => flow.ra = Some(addr.0.to_string()),
                                exporterIPv6Address(addr) => flow.ra = Some(addr.0.to_string()),
                                exporterIPv4Address(addr) => flow.ra = Some(addr.0.to_string()),

                                // Exporter ID ???
                                                                
                                // icmpTypeCodeIPv4(tyco) => {
                                //     if tyco.0 > 0 {
                                //         // println!("icmp type: {}, icmp code: {}", tyco.0 >> 8, tyco.0 & 0xFF);
                                //         flow.icmp_type = Some( ((tyco.0 >> 8) & 0xFF) as u8);
                                //         flow.icmp_code = Some((tyco.0 & 0xFF) as u8);
                                //     }
                                // }
                                _ => ()
                            }
                        }
                        // println!("Scope Fields: {:?}", record.scope_fields());
                        let j = serde_json::to_string(&flow).expect("Error serializing to json");
                        debug!("{}",j);
                        self.push(flow);
                    }
                }
                _ => () // Something else than Set
            }
        }


    }

    fn tcp_flags_as_string(&mut self, flg: u8) -> String {
        let mut flags = String::from("........");
            let fin = (flg & 0x01) != 0;
            let syn = (flg & 0x02) != 0;
            let rst = (flg & 0x04) != 0;
            let psh = (flg & 0x08) != 0;
            let ack = (flg & 0x10) != 0;
            let urg = (flg & 0x20) != 0;
            let ece = (flg & 0x40) != 0;
            let cwr = (flg & 0x80) != 0;

        if fin {
            flags.replace_range(7..8, "F")
        };
        if syn {
            flags.replace_range(6..7, "S")
        };
        if rst {
            flags.replace_range(5..6, "R")
        };
        if psh {
            flags.replace_range(4..5, "P")
        };
        if ack {
            flags.replace_range(3..4, "A")
        };
        if urg {
            flags.replace_range(2..3, "U")
        };
        if ece {
            flags.replace_range(1..2, "E")
        };
        if cwr {
            flags.replace_range(0..1, "C")
        };

        return flags;
    }


    pub fn push(&mut self, flow: FlowStats) {

        self.flows.push(flow);

        if self.flows.len() >= 1_000_000 {
            self.write_batch();
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

    fn rotate(&mut self, target:&str) {
        // Close current, rename, open new
        self.close_current();
        let filename = format!("{}/{}.current.parquet", self.base_dir.clone(), self.source_name.clone());
        // Rename to naming scheme
        let to_file = format!("{}/{}-{}.parquet", self.base_dir.clone(), self.source_name.clone(), target);
        debug!("rename {} -> {}", filename.clone(), to_file.clone());
        std::fs::rename(filename.clone(), to_file.clone()).unwrap();
        // Create new one
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
        let flowsrc = GenericStringArray::<i32>::from(vec![self.source_name.clone(); self.flows.len()]);

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

impl FlowStats {
    pub fn new() -> FlowStats {
        FlowStats { ..Default::default()}
    }

    pub fn create_fields() -> Vec<Field> {
        let mut fields: Vec<Field> = Vec::new();
    
        fields.push(Field::new("ts", Timestamp(TimeUnit::Microsecond, None), true));
        fields.push(Field::new("te", Timestamp(TimeUnit::Microsecond, None), true));
        fields.push(Field::new("sa", Utf8, true));
        fields.push(Field::new("da", Utf8, true));
        fields.push(Field::new("sp", UInt16, true));
        fields.push(Field::new("dp", UInt16, true));
        fields.push(Field::new("pr", Utf8, true));
        fields.push(Field::new("flg", Utf8, true));
        // fields.push(Field::new("icmp_type", UInt8, true));
        // fields.push(Field::new("icmp_code", UInt8, true));
        fields.push(Field::new("ipkt", UInt64, true));
        fields.push(Field::new("ibyt", UInt64, true));
        fields.push(Field::new("smk", UInt8, true));
        fields.push(Field::new("dmk", UInt8, true));
        fields.push(Field::new("ra", Utf8, true));
        fields.push(Field::new("inif", UInt16, true));
        fields.push(Field::new("outif", UInt16, true));
        fields.push(Field::new("sas", UInt32, true));
        fields.push(Field::new("das", UInt32, true));
        fields.push(Field::new("exid", UInt16, true));
        fields.push(Field::new("flowsrc", Utf8, true));

        return fields;
    }
    
   
}

