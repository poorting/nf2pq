use std::str::*;
use std::net::IpAddr;
use tokio::net::UdpSocket;
use anyhow::Error;
use crossbeam::channel;
use tracing::{instrument, info, debug};

use crate::flowprocessor::*;

// pub mod flowstats;

// #[derive(Debug)]
pub struct FlowCollector {
    source_ip: String,      // IP Address of the flow source
    source_name: String,
    port: u16,              // port to listen on
    rx      : tokio::sync::mpsc::UnboundedReceiver<String>,
    fp_tx   : tokio::sync::mpsc::UnboundedSender<FlowMessage>,
 }

impl FlowCollector {
    #[instrument]
    pub fn new(
        source_ip: Option<String>, 
        source_name: String, 
        port: u16,
        rx: tokio::sync::mpsc::UnboundedReceiver<String>,
        fp_tx: tokio::sync::mpsc::UnboundedSender<FlowMessage>,
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
    pub async fn start(&mut self) { 

        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.port))
            .await.expect("couldn't bind the udp socket");

        info!("Started listening for netflow from '{}' ({:?}) on port {}", self.source_name, self.source_ip, self.port);

        loop {
            // Read from Socket OR from channel; whichever occurs first
            let mut buf = [0; 65_535];

            // use async and tokio::select macro to avoid spinlocks
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    let (number_of_bytes, src_addr) = result.unwrap();
                    if  self.source_ip.len() == 0 || src_addr.ip() == IpAddr::from_str(&self.source_ip).unwrap() {
                        let filled_buf = &mut buf[..number_of_bytes];
                        let _ = self.fp_tx.send(FlowMessage::Datagram(filled_buf.to_owned()));
                    }
                }

                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            debug!("Received command: {}", cmd);
                            let _ = self.fp_tx.send(FlowMessage::Command(cmd));
                        }
                        None => {
                            // Channel has closed
                            break;
                        }
                    }
                }
            }
        }
        info!("flowcollector '{}' exiting gracefully", self.source_name);

    }

}


