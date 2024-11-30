use std::{collections::HashMap, fmt::Debug, net::IpAddr};
use bytes::BytesMut;
use netgauze_flow_pkt::{ie::Field, ipfix::IpfixPacket, netflow::NetFlowV9Packet};
use tokio_util::codec::Decoder;
use netgauze_flow_pkt::{
    codec::*, 
    netflow::Set,
    FlowInfo::NetFlowV9,
    FlowInfo::IPFIX,
    ie::Field::*,
    ie::protocolIdentifier,
};
use crossbeam::channel::{self};
use tracing::{info, debug, error};

use crate::flowstats::*;

// Exchange of information between collector and processor
// can contain message (e.g. rotate flow file) or received datagram
#[derive(Debug, Clone)]
pub enum FlowMessage {
    Command(String),
    Datagram(FlowPkt),
}

#[derive(Debug, Clone)]
pub struct FlowPkt {
    pub src_addr:IpAddr,
    pub dgram: Vec<u8>,
}

// #[derive(Debug)]
pub struct FlowProcessor {
    source_name : String,
    sample_itv  : u64, 
    rx          : channel::Receiver<FlowMessage>,
    tx          : channel::Sender<StatsMessage>,
    // parser      : FlowInfoCodec,
}

// #[derive(Debug, Default, Clone)]
impl FlowProcessor {
    pub fn new(
        source_name: String, 
        sample_itv: u64,
        rx: channel::Receiver<FlowMessage>,
        tx: channel::Sender<StatsMessage>,
    ) -> Result<FlowProcessor, std::io::Error> {

        let fp = FlowProcessor {
            source_name : source_name.to_string(),
            sample_itv  : sample_itv,
            rx          : rx,
            tx          : tx,
            // parser      : FlowInfoCodec::default(),
        };

        return Ok(fp);
    }

    pub fn start(&mut self) {
        // Listen to rx, 
        // check each message if it contains command or received datagram
        let mut packets_received:u64 = 0;
        let mut flows_received:u64 = 0;
        let mut exporters = HashMap::new();

        for msg in self.rx.clone().iter() {
            match msg {
                FlowMessage::Command(cmd) => {
                    if cmd.starts_with("quit") {
                        break;
                    } else {
                        debug!("received command: {}", cmd);
                    }
                }
                FlowMessage::Datagram(flowpkt) => {
                    // debug!("PACKET! ({} -> {})", flowpkt.src_addr.to_string(), packets_received);
                    packets_received += 1;
                    let mut bm_buf = BytesMut::with_capacity(0);
                    bm_buf.extend_from_slice(&flowpkt.dgram);
            
                    // let result = self.parser.decode(&mut bm_buf);
                    let result = exporters
                        .entry(flowpkt.src_addr.to_owned())
                        .or_insert(FlowInfoCodec::default())
                        .decode(&mut bm_buf);
                    match result {
                        Ok(Some(pkt)) => {
                            // debug!("{:#?}",pkt);
                            match pkt {
                                NetFlowV9(v9pkt) => {
                                    flows_received += self.process_v9packet(v9pkt, flowpkt.src_addr.to_string());
                                }
                                IPFIX(ipfix_pkt) => {
                                    // debug!("{:?}",ipfix_packet);
                                    flows_received += self.process_ipfix_packet(ipfix_pkt, flowpkt.src_addr.to_string());
                                }
                            }
                        }
                        Ok(None) => {
                            debug!("Ok(None) from parser.decode");
                        },
                        Err(error) => {
                            // error!("Error decoding flow packet: {:#?}",error);
                            match error {
                                FlowInfoCodecDecoderError::IpfixParsingError(_) => (),
                                FlowInfoCodecDecoderError::NetFlowV9ParingError(_) => (),
                                _ => error!("Error decoding flow packet: {:#?}",error),
                            }
                        }
                    }
                }
            }
        }

        info!("flowprocessor '{}' exiting gracefully ({} datagrams, {} flows received)", self.source_name, packets_received, flows_received);
    }


    fn process_ipfix_packet(&mut self, ipfix_pkt: IpfixPacket, router_ip: String) -> u64 {
        let mut flows_received:u64 = 0;
        // ipfix_pkt.observation_domain_id()
        for set in ipfix_pkt.sets() {
            // println!("{:?}", set);
            match set {
                netgauze_flow_pkt::ipfix::Set::Data { records, ..} => {
                    for record in records {
                        // debug!("{:?}", record);

                        // for scoped_field in record.scope_fields() {
                        //     // get some generic data from this
                        //     debug!("scoped_fields: {:?}", scoped_field);
                        // }
                        if record.scope_fields().len() > 0 {
                            // debug!("Scope: {:?}", record.fields());
                        } else {
                            flows_received += 1;
                            let mut flow = FlowStats::new();
                            flow.flowsrc = Some(self.source_name.clone());
                            flow.ra = Some(router_ip.clone());
                            self.process_flow_fields(record.fields(), &mut flow);
                            self.push(flow);
                            }
                        // debug!("{:?}", flow);
                    }
                }
                netgauze_flow_pkt::ipfix::Set::Template(_ot) => {
                    // debug!("{:#?}", ot);
                }
                netgauze_flow_pkt::ipfix::Set::OptionsTemplate(_ot) => {
                    // debug!("{:#?}", ot);
                }
            }
        }
        flows_received
    }

    fn process_v9packet(&mut self, v9pkt: NetFlowV9Packet, router_ip: String) -> u64 {
        let mut flows_received:u64 = 0;
        for set in v9pkt.sets() {
            // println!("{:?}", set);
            match set {
                Set::Data{ records, ..} => {
                    for record in records {
                        flows_received += 1;
                        let mut flow = FlowStats::new();
                        flow.flowsrc = Some(self.source_name.clone());
                        flow.ra = Some(router_ip.clone());
                        // println!("*********************************************");
                        self.process_flow_fields(record.fields(), &mut flow);
                        self.push(flow);
                    }
                }
                _ => () // Something else than Set
            }
        }
        flows_received
    }


    fn process_flow_fields(&mut self, fields: &Vec<Field>, flow: &mut FlowStats) {
        for field in fields {
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
                packetDeltaCount(cnt) =>  flow.ipkt = Some(cnt.0 * self.sample_itv),
                octetDeltaCount(cnt) => flow.ibyt = Some(cnt.0 * self.sample_itv),
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
                icmpTypeCodeIPv4(tyco) => {
                    if tyco.0 > 0 {
                        // println!("icmp type: {}, icmp code: {}", tyco.0 >> 8, tyco.0 & 0xFF);
                        flow.icmp_type = Some( ((tyco.0 >> 8) & 0xFF) as u8);
                        flow.icmp_code = Some((tyco.0 & 0xFF) as u8);
                    }
                }
                icmpTypeCodeIPv6(tyco) => {
                    if tyco.0 > 0 {
                        // println!("icmp type: {}, icmp code: {}", tyco.0 >> 8, tyco.0 & 0xFF);
                        flow.icmp_type = Some( ((tyco.0 >> 8) & 0xFF) as u8);
                        flow.icmp_code = Some((tyco.0 & 0xFF) as u8);
                    }
                }
                _ => ()
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
        self.tx.send(StatsMessage::Stats(flow)).unwrap();
    }


}


