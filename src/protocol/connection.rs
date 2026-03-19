use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use super::channel::Channel;
use super::packet::{Packet, read_packet, write_packet};

pub struct Connection {
    stream: Mutex<UnixStream>,
    pending_packets: Mutex<HashMap<u32, VecDeque<Packet>>>,
    next_channel_id: AtomicU32,
    channels: Mutex<HashMap<u32, ()>>,
    server_exited: AtomicBool,
}

impl Connection {
    pub fn new(stream: UnixStream) -> Arc<Self> {
        Arc::new(Self {
            stream: Mutex::new(stream),
            pending_packets: Mutex::new(HashMap::new()),
            // channel 0 is reserved for the control channel
            next_channel_id: AtomicU32::new(1),
            channels: Mutex::new(HashMap::new()),
            server_exited: AtomicBool::new(false),
        })
    }

    pub fn control_channel(self: &Arc<Self>) -> Channel {
        Channel::new(0, Arc::clone(self))
    }

    pub fn new_channel(self: &Arc<Self>) -> Channel {
        let next = self.next_channel_id.fetch_add(1, Ordering::SeqCst);
        // client channels use odd ids
        let channel_id = (next << 1) | 1;
        self.channels.lock().unwrap().insert(channel_id, ());
        Channel::new(channel_id, Arc::clone(self))
    }

    pub fn connect_channel(self: &Arc<Self>, channel_id: u32) -> Channel {
        self.channels.lock().unwrap().insert(channel_id, ());
        Channel::new(channel_id, Arc::clone(self))
    }

    pub fn mark_server_exited(&self) {
        self.server_exited.store(true, Ordering::SeqCst);
    }

    pub fn server_has_exited(&self) -> bool {
        self.server_exited.load(Ordering::SeqCst)
    }

    fn server_crashed_error() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            super::SERVER_CRASHED_MESSAGE,
        )
    }

    pub fn send_packet(&self, packet: &Packet) -> std::io::Result<()> {
        let mut stream = self.stream.lock().unwrap();
        match write_packet(&mut *stream, packet) {
            Ok(()) => Ok(()),
            Err(_) if self.server_has_exited() => Err(Self::server_crashed_error()),
            Err(e) => Err(e),
        }
    }

    pub fn receive_packet_for_channel(&self, channel_id: u32) -> std::io::Result<Packet> {
        // check pending packets first
        {
            let mut pending = self.pending_packets.lock().unwrap();
            if let Some(queue) = pending.get_mut(&channel_id) {
                if let Some(packet) = queue.pop_front() {
                    return Ok(packet);
                }
            }
        }

        // then read from stream until we get a packet for our channel, queuing for others
        loop {
            let packet = {
                let mut stream = self.stream.lock().unwrap();
                match read_packet(&mut *stream) {
                    Ok(p) => p,
                    Err(_) if self.server_has_exited() => {
                        return Err(Self::server_crashed_error());
                    }
                    Err(e) => return Err(e),
                }
            };

            if packet.channel == channel_id {
                return Ok(packet);
            }

            let mut pending = self.pending_packets.lock().unwrap();
            pending.entry(packet.channel).or_default().push_back(packet);
        }
    }

    pub fn close(&self) -> std::io::Result<()> {
        let stream = self.stream.lock().unwrap();
        stream.shutdown(std::net::Shutdown::Both)
    }
}
