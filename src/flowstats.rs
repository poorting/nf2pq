use arrow::datatypes::*;
use arrow::datatypes::DataType::*;

pub const IP_PROTO_STR:[&str;256] = [
    "HOPOPT", "ICMP", "IGMP", "GGP", "IPv4", "ST", "TCP", "CBT", "EGP", "IGP",  // 0..9
    "BBN-RCC-MON", "NVP-II", "PUP", "ARGUS", "EMCON", "XNET", "CHAOS", "UDP", "MUX", "DCN-MEAS", // 10..19
    "HMP", "PRM", "XNS-IDP", "TRUNK-1", "TRUNK-2", "LEAF-1", "LEAF-2", "RDP", "IRTP", "ISO-TP4", 
    "NETBLT", "MFE-NSP", "MERIT-INP", "DCCP", "3PC", "IDPR", "XTP", "DDP", "IDPR-CMTP", "TP++", 
    "IL", "IPv6", "SDRP", "IPv6-Route", "IPv6-Frag", "IDRP", "RSVP", "GRE", "DSR", "BNA", // 40..49
    "ESP", "AH", "I-NLSP", "SWIPE", "NARP", "Min-IPv4", "TLSP", "SKIP", "IPv6-ICMP", "IPv6-NoNxt", 
    "IPv6-Opts", "any host internal", "CFTP", "any local network", "SAT-EXPAK", "KRYPTOLAN", "RVD", "IPPC", "any DFS", "SAT-MON",
    "VISA", "IPCV", "CPNX", "CPHB", "WSN", "PVP", "BR-SAT-MON", "SUN-ND", "WB-MON", "WB-EXPAK", 
    "ISO-IP", "VMTP", "SECURE-VMTP", "VINES", "IPTM", "NSFNET-IGP", "DGP", "TCF", "EIGRP", "OSPFIGP", 
    "Sprite-RPC", "LARP", "MTP", "AX.25", "IPIP", "MICP", "SCC-SP", "ETHERIP", "ENCAP", "any private encryption scheme", // 90..99
    "GMTP", "IFMP", "PNNI", "PIM", "ARIS", "SCPS", "QNX", "A/N", "IPComp", "SNP", 
    "Compaq-Peer", "IPX-in-IP", "VRRP", "PGM", "any 0-hop protocol", "L2TP", "DDX", "IATP", "STP", "SRP", 
    "UTI", "SMP", "SM", "PTP", "ISIS over IPv4", "FIRE", "CRTP", "CRUDP", "SSCOPMCE", "IPLT", 
    "SPS", "PIPE", "SCTP", "FC", "RSVP-E2E-IGNORE", "Mobility Header", "UDPLite", "MPLS-in-IP", "manet", "HIP", 
    "Shim6", "WESP", "ROHC", "Ethernet", "AGGFRAG", "NSH", "Homa", "Unassigned", "Unassigned", "Unassigned", // 140..149
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", "Unassigned", 
    "Unassigned", "Unassigned", "Unassigned", "experimentation/testing", "experimentation/testing", "Reserved" // 250..255
];

#[derive(Debug, Clone)]
pub enum StatsMessage {
    Command(String),
    Stats(FlowStats),
}

#[derive(Default, Debug, Clone)]
pub struct FlowStats {
    pub ts      : Option<i64>,
    pub te      : Option<i64>,
    pub sa      : Option<String>,
    pub da      : Option<String>,
    pub sp      : Option<u16>,
    pub dp      : Option<u16>,
    pub pr      : Option<u8>,
    pub prs     : Option<String>,
    pub flg     : Option<String>,
    pub icmp_type: Option<u8>,
    pub icmp_code: Option<u8>,
    pub pkt     : Option<u64>,
    pub byt     : Option<u64>,
    pub smk     : Option<u8>,
    pub dmk     : Option<u8>,
    pub ra      : Option<String>,
    pub nh      : Option<String>,
    pub inif    : Option<u16>,
    pub outif   : Option<u16>,
    pub sas     : Option<u32>,
    pub das     : Option<u32>,
    // pub exid    : Option<u16>,
    pub flowsrc : Option<String>,
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
        fields.push(Field::new("pr", UInt8, true));
        fields.push(Field::new("prs", Utf8, true));
        fields.push(Field::new("flg", Utf8, true));
        fields.push(Field::new("icmp_type", UInt8, true));
        fields.push(Field::new("icmp_code", UInt8, true));
        fields.push(Field::new("pkt", UInt64, true));
        fields.push(Field::new("byt", UInt64, true));
        fields.push(Field::new("smk", UInt8, true));
        fields.push(Field::new("dmk", UInt8, true));
        fields.push(Field::new("ra", Utf8, true));
        fields.push(Field::new("nh", Utf8, true));
        fields.push(Field::new("in", UInt16, true));
        fields.push(Field::new("out", UInt16, true));
        fields.push(Field::new("sas", UInt32, true));
        fields.push(Field::new("das", UInt32, true));
        fields.push(Field::new("flowsrc", Utf8, true));

        return fields;
    }
}
