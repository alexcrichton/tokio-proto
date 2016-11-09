//! Dispatch for multiplexed protocols
//!
//! This module contains reusable components for quickly implementing clients
//! and servers for multiplex based protocols.
//!
//! ## Multiplexing
//!
//! Multiplexing allows multiple request / response transactions to be inflight
//! concurrently while sharing the same underlying transport. This is usually
//! done by splitting the protocol into frames and assigning a request ID to
//! each frame.
//!
//! ## Considerations
//!
//! There are some difficulties with implementing back pressure in the case
//! that the wire protocol does not support a means by which backpressure can
//! be signaled to the peer.
//!
//! The problem is that, the transport is no longer read from while waiting for
//! the current frames to be processed, it is possible that the processing
//! logic is blocked waiting for another frame that is currently pending on the
//! socket.
//!
//! To deal with this, once the connection level frame buffer is filled, a
//! timeout is set. If no further frames are able to be read before the timeout
//! expires, then the connection is killed.
//!
//! ## Current status
//!
//! As of now, the implementation only supports multiplexed requests &
//! responses without streaming bodies.

mod frame_buf;
mod client;
mod multiplex;
mod server;

pub use self::multiplex::{Multiplex, MultiplexMessage, Dispatch};
pub use self::client::connect;
pub use self::server::Server;

/// Identifies a request / response thread
pub type RequestId = u64;

/// A multiplexed protocol frame
#[derive(Debug, Clone)]
pub enum Frame<T, B, E> {
    /// Either a request or a response.
    Message {
        /// Message exchange identifier
        id: RequestId,
        /// The message value
        message: T,
        /// Set to true when body frames will follow with the same request ID.
        body: bool,
        /// Set to `true` when this message does not have a pair in the other
        /// direction
        solo: bool,
    },
    /// Body frame.
    Body {
        /// Message exchange identifier
        id: RequestId,
        /// Body chunk. Setting to `None` indicates that the body is done
        /// streaming and there will be no further body frames sent with the
        /// given request ID.
        chunk: Option<B>,
    },
    /// Error
    Error {
        /// Message exchange identifier
        id: RequestId,
        /// Error value
        error: E,
    },
}

/*
 *
 * ===== impl Frame =====
 *
 */

impl<T, B, E> Frame<T, B, E> {
    /// Return the request ID associated with the frame.
    pub fn request_id(&self) -> Option<RequestId> {
        match *self {
            Frame::Message { id, .. } => Some(id),
            Frame::Body { id, .. } => Some(id),
            Frame::Error { id, .. } => Some(id),
        }
    }

    /// Unwraps a frame, yielding the content of the `Message`.
    pub fn unwrap_msg(self) -> T {
        match self {
            Frame::Message { message, .. } => message,
            Frame::Body { .. } => panic!("called `Frame::unwrap_msg()` on a `Body` value"),
            Frame::Error { .. } => panic!("called `Frame::unwrap_msg()` on an `Error` value"),
        }
    }

    /// Unwraps a frame, yielding the content of the `Body`.
    pub fn unwrap_body(self) -> Option<B> {
        match self {
            Frame::Body { chunk, .. } => chunk,
            Frame::Message { .. } => panic!("called `Frame::unwrap_body()` on a `Message` value"),
            Frame::Error { .. } => panic!("called `Frame::unwrap_body()` on an `Error` value"),
        }
    }

    /// Unwraps a frame, yielding the content of the `Error`.
    pub fn unwrap_err(self) -> E {
        match self {
            Frame::Error { error, .. } => error,
            Frame::Body { .. } => panic!("called `Frame::unwrap_err()` on a `Body` value"),
            Frame::Message { .. } => panic!("called `Frame::unwrap_err()` on a `Message` value"),
        }
    }
}
