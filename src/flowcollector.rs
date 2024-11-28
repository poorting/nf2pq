use std::net::UdpSocket;
use crossbeam::channel;
use tracing::{instrument, info, error};

use crate::flowprocessor::*;

// #[derive(Debug)]
pub struct FlowCollector {
    source_ip   : String,      // IP Address of the flow source
    source_name : String,
    port        : u16,         // port to listen on
    fp_tx       : channel::Sender<FlowMessage>,
    socket      : UdpSocket,
 }

impl FlowCollector {
    #[instrument]
    pub fn new(
        source_ip: Option<String>, 
        source_name: String, 
        port: u16,
        fp_tx: channel::Sender<FlowMessage>,
    ) -> Result<FlowCollector, std::io::Error> {
    
        let srcip:String = match source_ip {
            Some(sip) => sip,
            _ => "".to_string(),            
        };
        let socket_r = UdpSocket::bind(format!("0.0.0.0:{}", port));
        match socket_r {
            Err(e) => {
                error!("Error creating socket: {:?}", e);
                return Err(e);
            }
            _ => (),
        }

        let fc = FlowCollector {
            source_ip   : srcip.clone(),
            source_name : source_name.clone(),
            port        : port,
            fp_tx       : fp_tx,
            socket      : socket_r.unwrap(),
        };
        
        Ok(fc)
    }

    // #[instrument]
    pub fn start(&mut self) {

        info!("Started listening for netflow from '{}' (ip: {}) on port {}", self.source_name, self.source_ip, self.port);

        let mut packets_received = 0;
        let mut buf = [0u8; 65_535];
        loop {
            // Read from Socket 
            match self.socket.recv_from(&mut buf) {
                Ok((number_of_bytes, _src_addr )) => {
                    packets_received += 1;
                    let filled_buf = &mut buf[..number_of_bytes];
                    let result = self.fp_tx.send(FlowMessage::Datagram(filled_buf.to_vec()));
                    match result {
                        Err(_) => {
                            break;
                        }
                        _ => (),
                    }
                }

                Err(e) => {
                    error!("{}", e)
                }
            }
        }
        info!("flowcollector '{}' exiting gracefully ({} datagrams received)", self.source_name, packets_received);

    }

}


