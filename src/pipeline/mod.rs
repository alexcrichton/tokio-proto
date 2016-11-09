//! A dispatcher for pipelining protocols
//!
//! This module contains reusable components for quickly implementing clients
//! and servers for pipeline based protocols.
//!
//! # Pipelining
//!
//! Protocol pipelining is a technique in which multiple requests are written
//! out to a single destination without waiting for their corresponding
//! responses. Pipelining is used in a multitude of different protocols such as
//! HTTP/1.1 and Redis in order to increase throughput on a single connection.
//!
//! Pipelining with the max number of in-flight requests set to 1 implies that
//! for each request, the response must be received before sending another
//! request on the same connection.
//!
//! Another protocol dispatching strategy is multiplexing (which will be
//! included in Tokio soon).
//!
//! # Usage
//!
//! Both the server and client pipeline dispatchers take a generic `Transport`
//! that reads and writes `Frame` messages. It operates on the transport
//! following the rules of pipelining as described above and exposes the
//! protocol using a `Service`.

mod client;
mod server;
mod pipeline;

pub use self::pipeline::{Pipeline, PipelineMessage, Dispatch};
pub use self::server::Server;
pub use self::client::connect;

/// A pipelined protocol frame
#[derive(Debug, Clone)]
pub enum Frame<T, B, E> {
    /// Either a request or a response
    Message {
        /// The message value
        message: T,
        /// Set to true when body frames will follow
        body: bool,
    },
    /// Body frame. None indicates that the body is done streaming.
    Body {
        /// Body chunk. Setting to `None` indicates that the body is done
        /// streaming and there will be no further body frames sent with the
        /// given request ID.
        chunk: Option<B>,
    },
    /// Error
    Error {
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
            Frame::Body { chunk } => chunk,
            Frame::Message { .. } => panic!("called `Frame::unwrap_body()` on a `Message` value"),
            Frame::Error { .. } => panic!("called `Frame::unwrap_body()` on an `Error` value"),
        }
    }

    /// Unwraps a frame, yielding the content of the `Error`.
    pub fn unwrap_err(self) -> E {
        match self {
            Frame::Error { error } => error,
            Frame::Body { .. } => panic!("called `Frame::unwrap_err()` on a `Body` value"),
            Frame::Message { .. } => panic!("called `Frame::unwrap_err()` on a `Message` value"),
        }
    }
}
