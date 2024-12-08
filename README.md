# nf2pq
IPFix/Netflow v9 flow collector that writes flow information to parquet and optionally inserts into ClickHouse.



| *name*    | *description*                                                                                     | *data type* |
|-----------|---------------------------------------------------------------------------------------------------|-------------|
| ts        | Start time of the flow                                                                            | DateTime64  |
| te        | End time of the flow                                                                              | DateTime64  |
| sa        | Source IP address                                                                                 | String      |
| da        | Destination IP address                                                                            | String      |
| sp        | Source port                                                                                       | UInt16      |
| dp        | Destination port                                                                                  | UInt16      |
| pr        | IP Protocol number                                                                                | UInt8       |
| prs       | IP Protocol name (e.g. 'TCP' or 'UDP')                                                            | String      |
| flg       | Flags (if pr/prs is 6/'TCP')                                                                      | String      |
| icmp_type | The ICMP Type (if pr/prs is 1/'ICMP')                                                             | UInt8       |
| icmp_code | The ICMP Code (if pr/prs is 1/'ICMP')                                                             | UInt8       |
| ipkt      | Number of packets in this flow                                                                    | UInt64      |
| ibyt      | Number of bytes in this flow                                                                      | UInt64      |
| smk       | Source mask                                                                                       | UInt8       |
| dmk       | Destination mask                                                                                  | UInt8       |
| ra        | IP address of the router/network device that exported this flow information                       | String      |
| nh        | IP address of the Next Hop                                                                        | String      |
| in        | Input interface number                                                                            | UInt16      |
| out       | Output interface number                                                                           | UInt16      |
| sas       | Source AS number                                                                                  | UInt16      |
| das       | Destination AS number                                                                             | UInt16      |
| flowsrc   | Additional label added by nf2pq. <br/>Can be set in the config file per flow exporter             | String      |
