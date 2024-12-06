use std::{collections::{BTreeMap, HashMap}, fmt::Debug, net::IpAddr};
use crossbeam::channel::{self};
use tracing::{debug, info};
use netflow_parser::{variable_versions::{data_number::{DataNumber, FieldValue}, ipfix::IPFix, ipfix_lookup::IPFixField, v9::V9, v9_lookup::V9Field}, NetflowPacket, NetflowParser};
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
        let mut exporters: HashMap<IpAddr, NetflowParser> = HashMap::new();

        // let mut parser = NetflowParser::default();

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
                    packets_received += 1;
                    // let mut bm_buf = BytesMut::with_capacity(0);
                    // bm_buf.extend_from_slice(&flowpkt.dgram);

                    let result = match exporters.get_mut(&flowpkt.src_addr) {
                        Some(parser) => parser.parse_bytes(flowpkt.dgram.as_slice()),
                        None => {
                            let mut new_parser = NetflowParser::default();
                            let result = new_parser.parse_bytes(flowpkt.dgram.as_slice());
                            exporters.insert(flowpkt.src_addr, new_parser);
                            result
                        }
                    };
            
                    // debug!("parsed: {:?}", result);
                    for nfpacket in result {
                        match nfpacket {
                            NetflowPacket::IPFix(ipfix) => {
                                flows_received += self.process_ipfix_packet(ipfix, flowpkt.src_addr.to_string());
                            }
                            NetflowPacket::V9(v9pkt) => {
                                flows_received += self.process_v9_packet(v9pkt, flowpkt.src_addr.to_string());
                            }
                            _ => (),                    
                        }
                    }
                }
            }
        }
        info!("flowprocessor '{}' exiting gracefully ({} datagrams, {} flows received)", self.source_name, packets_received, flows_received);
    }

    fn process_ipfix_packet(&mut self, ipfix:IPFix, exporter_ip: String) -> u64 {
        let mut flows_received:u64 = 0;
        for flowset in ipfix.flowsets.iter() {
        if flowset.body.options_data.is_some() {
            // debug!("options data: {:?}",flowset.body);
        } else {
            for flows_data in flowset.body.data.iter() {
                for flow_fields in flows_data.data_fields.iter() {
                    flows_received += 1;
                    let mut flow = FlowStats::new();
                    flow.flowsrc = Some(self.source_name.clone());
                    flow.ra = Some(exporter_ip.clone());
                    self.process_flow_fields(flow_fields, &mut flow);
                    self.push(flow);
                    }
                }
            }
        }
        return flows_received;
    }

    fn process_flow_fields(&mut self, fields:&BTreeMap<usize, (IPFixField, FieldValue)> , flow: &mut FlowStats) {
        for (_fieldnr, field) in fields.iter() {
            // debug!("{:?}", field);
            match field {
                (IPFixField::FlowStartMilliseconds, FieldValue::Duration(d)) => flow.ts = Some(d.as_micros() as i64),
                (IPFixField::FlowEndMilliseconds, FieldValue::Duration(d)) => flow.te = Some(d.as_micros() as i64),
                (IPFixField::SourceIpv4address, FieldValue::Ip4Addr(a)) => flow.sa = Some(a.to_string()),
                (IPFixField::DestinationIpv4address, FieldValue::Ip4Addr(a)) => flow.da = Some(a.to_string()),
                (IPFixField::SourceIpv6address, FieldValue::Ip6Addr(a)) => flow.sa = Some(a.to_string()),
                (IPFixField::DestinationIpv6address, FieldValue::Ip6Addr(a)) => flow.da = Some(a.to_string()),
                (IPFixField::IpNextHopIpv4address, FieldValue::Ip4Addr(a)) => flow.nh = Some(a.to_string()),
                (IPFixField::IpNextHopIpv6address, FieldValue::Ip6Addr(a)) => flow.nh = Some(a.to_string()),
                (IPFixField::SourceTransportPort, FieldValue::DataNumber(DataNumber::U16(p))) => flow.sp = Some(*p),
                (IPFixField::DestinationTransportPort, FieldValue::DataNumber(DataNumber::U16(p))) => flow.dp = Some(*p),
                (IPFixField::OctetDeltaCount, FieldValue::DataNumber(DataNumber::U64(c))) => flow.byt = Some(*c * self.sample_itv),
                (IPFixField::PacketDeltaCount, FieldValue::DataNumber(DataNumber::U64(c))) => flow.pkt = Some(*c * self.sample_itv),
                (IPFixField::ProtocolIdentifier, FieldValue::DataNumber(DataNumber::U8(b))) => flow.pr = Some(*b),
                (IPFixField::TcpControlBits, FieldValue::DataNumber(DataNumber::U8(b))) => flow.flg = Some(self.tcp_flags_as_string(*b)),
                (IPFixField::SourceIpv4prefixLength, FieldValue::DataNumber(DataNumber::U8(b))) => flow.smk = Some(*b),
                (IPFixField::DestinationIpv4prefixLength, FieldValue::DataNumber(DataNumber::U8(b))) => flow.dmk = Some(*b),
                (IPFixField::SourceIpv6prefixLength, FieldValue::DataNumber(DataNumber::U8(b))) => flow.smk = Some(*b),
                (IPFixField::DestinationIpv6prefixLength, FieldValue::DataNumber(DataNumber::U8(b))) => flow.dmk = Some(*b),
                (IPFixField::BgpSourceAsNumber, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.sas = Some(*nr),
                (IPFixField::BgpDestinationAsNumber, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.das = Some(*nr),
                (IPFixField::IngressInterface, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.inif = Some(*nr as u16),
                (IPFixField::EgressInterface, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.outif = Some(*nr as u16),
                (IPFixField::IcmpTypeCodeIpv4, FieldValue::DataNumber(DataNumber::U16(tc))) => {
                    flow.icmp_type = Some( ((*tc >> 8) & 0xFF) as u8);
                    flow.icmp_code = Some((*tc & 0xFF) as u8);
                }
                (IPFixField::IcmpTypeCodeIpv6, FieldValue::DataNumber(DataNumber::U16(tc))) => {
                    flow.icmp_type = Some( ((*tc >> 8) & 0xFF) as u8);
                    flow.icmp_code = Some((*tc & 0xFF) as u8);
                }
                // (IPFixField::IngressInterface, value) => {debug!("flg: {:?}", value);},
                _ => (),
                
            }
        }
    }

    fn process_v9_packet(&mut self, v9pkt:V9, exporter_ip: String) -> u64 {
        let mut flows_received:u64 = 0;
        for flowset in v9pkt.flowsets.iter() {
        if flowset.body.options_data.is_some() {
            // debug!("options data: {:?}",flowset.body);
        } else {
            for flows_data in flowset.body.data.iter() {
                for flow_fields in flows_data.data_fields.iter() {
                    flows_received += 1;
                    let mut flow = FlowStats::new();
                    flow.flowsrc = Some(self.source_name.clone());
                    flow.ra = Some(exporter_ip.clone());
                    self.process_v9_flow_fields(flow_fields, &mut flow);
                    self.push(flow);
                    }
                }
            }
        }
        return flows_received;
    }

    fn process_v9_flow_fields(&mut self, fields:&BTreeMap<usize, (V9Field, FieldValue)> , flow: &mut FlowStats) {
        for (_fieldnr, field) in fields.iter() {
            // debug!("{:?}", field);
            match field {
                (V9Field::FlowStartMilliseconds, FieldValue::Duration(d)) => flow.ts = Some(d.as_micros() as i64),
                (V9Field::FlowEndMilliseconds, FieldValue::Duration(d)) => flow.te = Some(d.as_micros() as i64),
                (V9Field::Ipv4SrcAddr, FieldValue::Ip4Addr(a)) => flow.sa = Some(a.to_string()),
                (V9Field::Ipv4DstAddr, FieldValue::Ip4Addr(a)) => flow.da = Some(a.to_string()),
                (V9Field::Ipv6SrcAddr, FieldValue::Ip6Addr(a)) => flow.sa = Some(a.to_string()),
                (V9Field::Ipv6DstAddr, FieldValue::Ip6Addr(a)) => flow.da = Some(a.to_string()),
                (V9Field::Ipv4NextHop, FieldValue::Ip4Addr(a)) => flow.nh = Some(a.to_string()),
                (V9Field::Ipv6NextHop, FieldValue::Ip6Addr(a)) => flow.nh = Some(a.to_string()),
                (V9Field::L4SrcPort, FieldValue::DataNumber(DataNumber::U16(p))) => flow.sp = Some(*p),
                (V9Field::L4DstPort, FieldValue::DataNumber(DataNumber::U16(p))) => flow.dp = Some(*p),
                (V9Field::InBytes, FieldValue::DataNumber(DataNumber::U64(c))) => flow.byt = Some(*c * self.sample_itv),
                (V9Field::InPkts, FieldValue::DataNumber(DataNumber::U64(c))) => flow.pkt = Some(*c * self.sample_itv),
                (V9Field::Protocol, FieldValue::DataNumber(DataNumber::U8(b))) => flow.pr = Some(*b),
                (V9Field::TcpFlags, FieldValue::DataNumber(DataNumber::U8(b))) => flow.flg = Some(self.tcp_flags_as_string(*b)),
                (V9Field::SrcMask, FieldValue::DataNumber(DataNumber::U8(b))) => flow.smk = Some(*b),
                (V9Field::DstMask, FieldValue::DataNumber(DataNumber::U8(b))) => flow.dmk = Some(*b),
                (V9Field::Ipv6SrcMask, FieldValue::DataNumber(DataNumber::U8(b))) => flow.smk = Some(*b),
                (V9Field::Ipv6DstMask, FieldValue::DataNumber(DataNumber::U8(b))) => flow.dmk = Some(*b),
                (V9Field::SrcAs, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.sas = Some(*nr),
                (V9Field::DstAs, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.das = Some(*nr),
                (V9Field::InputSnmp, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.inif = Some(*nr as u16),
                (V9Field::OutputSnmp, FieldValue::DataNumber(DataNumber::U32(nr))) => flow.outif = Some(*nr as u16),
                (V9Field::IcmpType, FieldValue::DataNumber(DataNumber::U16(tc))) => {
                    flow.icmp_type = Some( ((*tc >> 8) & 0xFF) as u8);
                    flow.icmp_code = Some((*tc & 0xFF) as u8);
                }
                (V9Field::IcmpIpv6TypeValue, FieldValue::DataNumber(DataNumber::U16(tc))) => {
                    flow.icmp_type = Some( ((*tc >> 8) & 0xFF) as u8);
                    flow.icmp_code = Some((*tc & 0xFF) as u8);
                }
                // (v9t, value) => {debug!(" {:?} -> {:?}", v9t, value);},
                _ => (),
                
            }
        }
    }

    fn tcp_flags_as_string(&mut self, flg: u8) -> String {
        let flags_src = "CEUAPRSF";
        let mut flags = String::from("........");
        for i in 0..=7 as usize{
            if (flg & 2_u8.pow(i as u32)) !=0 {
                flags.replace_range(7-i..=7-i, &flags_src[7-i..=7-i]);
            }
        }
        return flags;
    }


    pub fn push(&mut self, flow: FlowStats) {
        self.tx.send(StatsMessage::Stats(flow)).unwrap();
    }


}


