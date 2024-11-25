use std::{net::UdpSocket, str::*};
use std::net::IpAddr;
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

        loop {
            // Read from Socket OR from channel; whichever occurs first
            let mut buf = [0; 65_535];
            match self.socket.recv_from(&mut buf) {
                Ok((number_of_bytes, src_addr )) => {
                    // println!("received data!");
                    // if  self.source_ip.len() == 0 || 
                    //     src_addr.ip() == IpAddr::from_str(&self.source_ip).unwrap() ||
                    //     src_addr.ip().is_loopback() {
                        let filled_buf = &mut buf[..number_of_bytes];
                        let result = self.fp_tx.send(FlowMessage::Datagram(filled_buf.to_owned()));
                        match result {
                            Err(_) => {
                                break;
                            }
                            _ => (),
                        }
                    // }
                }

                Err(e) => {
                    error!("{}", e)
                }
            }
        }
        info!("flowcollector '{}' exiting gracefully", self.source_name);

    }

}


