use std::fmt::Debug;
// use std::sync::Arc;
// use std::fs::File;
use bytes::BytesMut;
// use arrow::datatypes::*;
use netgauze_flow_pkt::netflow::NetFlowV9Packet;
use tokio_util::codec::Decoder;
// use parquet::{
//     basic::{Compression, Encoding},
//     file::properties::*,
//     arrow::ArrowWriter,
// };
use netgauze_flow_pkt::{
    codec::FlowInfoCodec, 
    netflow::Set,
    FlowInfo::NetFlowV9,
    ie::Field::*,
    ie::protocolIdentifier,
};
// use serde::{Deserialize, Serialize};
use crossbeam::channel::{self};
use tracing::{info, debug, error};
// use clickhouse::{Client, inserter, Row};

use crate::flowstats::*;
use crate::flowwriter::*;

// Exchange of information between collector and processor
// can contain message (e.g. rotate flow file) or received datagram
#[derive(Debug, Clone)]
pub enum FlowMessage {
    Command(String),
    Datagram(Vec<u8>),
}


// #[derive(Debug)]
pub struct FlowProcessor {
    source_name : String, 
    // base_dir    : String,
    rx          : channel::Receiver<FlowMessage>,
    // schema      : Schema,         // Schema of the parquet file
    // writer      : ArrowWriter<std::fs::File>,
    parser      : FlowInfoCodec,
    // flows       : Vec<FlowStats>,  // Will contain flowstats
    flow_writer  : Option<FlowWriter>,
}


impl FlowProcessor {
    pub fn new(
        source_name: String, 
        // base_dir: String,
        rx: channel::Receiver<FlowMessage>,
        flow_writer: Option<FlowWriter>,
    ) -> Result<FlowProcessor, std::io::Error> {

        let fp = FlowProcessor {
            source_name : source_name.to_string(),
            // base_dir    : base_dir.to_string(),
            rx          : rx,
            parser      : FlowInfoCodec::default(),
            flow_writer  : flow_writer,
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
                        if let Some(fw) = self.flow_writer.as_mut() {
                            fw.rotate_tick(true);
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
        // loop {
        //     match self.rx.recv().await {
        //         Some(msg) => {
        //             match msg {
        //                 FlowMessage::Command(cmd) => {
        //                     if cmd.starts_with("flush") {
        //                         debug!("Received command: {}", cmd.clone());
        //                         let mut parts = cmd.split_whitespace();
        //                         if let Some(fw) = self.flow_writer.as_mut() {
        //                             fw.rotate(parts.nth(1).unwrap());
        //                         }
        //                     } else {
        //                         println!("received command: {}", cmd);
        //                     }
        //                 }
        //                 FlowMessage::Datagram(udp) => {
        //                     let mut bm_buf = BytesMut::with_capacity(0);
        //                     bm_buf.extend_from_slice(&udp);
                    
        //                     let result = self.parser.decode(&mut bm_buf);
                    
        //                     match result {
        //                         Ok(Some(pkt)) => {
        //                             match pkt {
        //                                 NetFlowV9(v9pkt) => {
        //                                     self.process_v9packet(v9pkt).await;
        //                                 }
        //                                 _ => () // ignore everything else (IPFIX)
        //                             }
        //                         }
        //                         Ok(None) => {
        //                             debug!("Ok(None) from parser.decode");
        //                         },
        //                         Err(error) => {
        //                             error!("Error decoding flow packet: {:?}",error);
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //         None => {
        //             break;
        //         }
        //     }

        // };

        info!("flowprocessor '{}' exiting gracefully", self.source_name);
        if let Some(fw) = self.flow_writer.as_mut() {
            // fw.close_current();
            fw.rotate_tick(false);
        }

    }

    fn process_v9packet(&mut self, v9pkt: NetFlowV9Packet) {
        for set in v9pkt.sets() {
            // println!("{:?}", set);
            match set {
                Set::Data{ records, ..} => {
                    for record in records {
                        let mut flow = FlowStats::new();
                        flow.flowsrc = Some(self.source_name.clone());
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
                        // let j = serde_json::to_string(&flow).expect("Error serializing to json");
                        // debug!("{}",j);
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

        if let Some(fw) = self.flow_writer.as_mut() {
            fw.push(flow.clone());
        }
    }


}


