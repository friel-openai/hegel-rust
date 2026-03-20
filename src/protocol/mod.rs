mod channel;
mod connection;
mod packet;

pub use channel::Channel;
pub use connection::Connection;

pub const HANDSHAKE_STRING: &[u8] = b"hegel_handshake_start";

pub const SERVER_CRASHED_MESSAGE: &str = "The hegel server process exited unexpectedly. \
     See .hegel/server.log for diagnostic information.";
