use std::net::UdpSocket;
use crossbeam::channel;
use tracing::{info, debug, error};

use crate::flowprocessor::*;

// #[derive(Debug)]
/// A flow collector that simply listens to a UDP port and immediately dumps a datagram into a channel
pub struct FlowCollector {
    /// Name of this flow exporter, this is inserted into the flowsrc column
    source_name : String,
    /// Port to listen to
    port        : u16,
    /// Channel to a FlowProcessor that will proces netflow datagrams
    fp_tx       : channel::Sender<FlowMessage>,
    /// The socket used for listening to UDP
    socket      : UdpSocket,
 }

impl FlowCollector {
    pub fn new(
        source_name: String, 
        port: u16,
        fp_tx: channel::Sender<FlowMessage>,
    ) -> Result<FlowCollector, std::io::Error> {
    
        let socket_r = UdpSocket::bind(format!("0.0.0.0:{}", port));
        match socket_r {
            Err(e) => {
                error!("Error creating socket: {:?}", e);
                return Err(e);
            }
            _ => (),
        }

        let fc = FlowCollector {
            source_name : source_name.clone(),
            port        : port,
            fp_tx       : fp_tx,
            socket      : socket_r.unwrap(),
        };
        
        Ok(fc)
    }

    pub fn start(&mut self) {

        info!("Started listening for netflow from '{}' on port {}", self.source_name, self.port);

        let mut packets_received = 0;
        let mut buf = [0u8; 65_535];
        loop {
            // Read from Socket 
            match self.socket.recv_from(&mut buf) {
                Ok((number_of_bytes, src_addr )) => {
                    packets_received += 1;
                    let filled_buf = &mut buf[..number_of_bytes];
                    let result = self.fp_tx.send(
                        FlowMessage::Datagram(FlowPkt{src_addr: src_addr.ip(), dgram: filled_buf.to_vec()}));
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
        debug!("flowcollector '{}' exiting gracefully ({} datagrams received)", self.source_name, packets_received);

    }

}


