use std::str::*;
use std::net::{IpAddr, UdpSocket};
use std::io::ErrorKind;
use anyhow::Error;
use crossbeam::channel;
use tracing::{instrument, info, debug, error};

use crate::flowprocessor::*;

// pub mod flowstats;

// #[derive(Debug)]
pub struct FlowCollector {
    source_ip: String,      // IP Address of the flow source
    source_name: String,
    port: u16,              // port to listen on
    rx      : channel::Receiver<String>,
    fp_tx   : channel::Sender<FlowMessage>,
 }

impl FlowCollector {
    #[instrument]
    pub fn new(
        source_ip: Option<String>, 
        source_name: String, 
        port: u16,
        rx: channel::Receiver<String>,
        fp_tx: channel::Sender<FlowMessage>,
    ) -> Result<FlowCollector, Error> {
    
        let srcip:String = match source_ip {
            Some(sip) => sip,
            _ => "".to_string(),            
        };
        let fc = FlowCollector {
            source_ip: srcip.clone(),
            source_name: source_name.clone(),
            port: port,
            rx: rx,
            fp_tx: fp_tx,
        };
        
        Ok(fc)
    }

    // #[instrument]
    pub fn start(&mut self) { 

        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.port))
                                .expect("couldn't bind to address");
        let _ = socket.set_nonblocking(true);

        info!("Started listening for netflow from '{}' ({:?}) on port {}", self.source_name, self.source_ip, self.port);

        loop {
            // Read from Socket
            let mut buf = [0; 65_535];
            match socket.recv_from(&mut buf) {
                Ok((number_of_bytes, src_addr )) => {
                    // println!("received data!");
                    if  self.source_ip.len() == 0 || src_addr.ip() == IpAddr::from_str(&self.source_ip).unwrap() {
                        let filled_buf = &mut buf[..number_of_bytes];
                        let _ = self.fp_tx.send(FlowMessage::Datagram(filled_buf.to_owned()));
                    }
                }

                Err(e) => {
                    if !matches!(e.kind(), ErrorKind::WouldBlock) {
                        eprintln!("{}", e)
                    }
                }
            }

            // Now see if there is a command.
            match self.rx.try_recv() {
                Ok(cmd) => {
                    debug!("Received command: {}", cmd);
                    let _ = self.fp_tx.send(FlowMessage::Command(cmd));
                }
                Err(e ) => {
                    if e.is_disconnected() {
                        break;
                    }
                }
            }
        }
        info!("flowcollector '{}' exiting gracefully", self.source_name);

    }

}


