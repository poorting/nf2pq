use arrow::datatypes::*;
// use arrow::array::*;
use arrow::datatypes::DataType::*;
// use parquet::{
//     basic::{Compression, Encoding},
//     file::properties::*,
//     arrow::ArrowWriter,
// };
// use serde::{Deserialize, Serialize};
// use clickhouse::Row;

#[derive(Default, Debug, Clone)]
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
