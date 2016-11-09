//! An "easy" multiplexing module
//!
//! This module is the same as the top-level `multiplex` module in this crate
//! except that it has no support for streaming bodies. This in turn simplifies
//! a number of generics and allows for protocols to easily get off the ground
//! running.
//!
//! The API surface area of this module is the same as the top-level one, so no
//! new functionality is introduced here. In fact all APIs here are built on top
//! of the `multiplex` module itself, they just simplify the generics in play and
//! such.

use std::io;
use std::marker;

use futures::stream::Empty;
use futures::{Future, Poll, Stream, Sink, AsyncSink, StartSend};
use tokio_core::reactor::Handle;
use tokio_service::Service;

use easy::EasyClient;
use multiplex::{self, RequestId};
use {Message, Body};

/// The "easy" form of connecting a multiplexed client.
///
/// This function takes an instance of the `tokio_core::io::FrameIo` trait as
/// the `frames` argument, a `handle` to the event loop to connect on, and then
/// returns the connected client.
///
/// The client returned implements the `Service` trait. This trait
/// implementation allows sending requests to the transport provided and returns
/// futures to the responses. All requests are automatically multiplexed and
/// managed internally.
pub fn connect<F, T, U>(frames: F, handle: &Handle)
    -> EasyClient<U, T>
    where F: Stream<Item = (RequestId, T), Error = io::Error> +
             Sink<SinkItem = (RequestId, U), SinkError = io::Error> +
             'static,
          T: 'static,
          U: 'static,
{
    EasyClient {
        inner: multiplex::connect(MyTransport::new(frames), handle),
    }
}

struct MyTransport<F, T, U> {
    inner: F,
    _marker: marker::PhantomData<fn() -> (T, U)>,
}

impl<F, T, U> MyTransport<F, T, U> {
    fn new(f: F) -> MyTransport<F, T, U> {
        MyTransport {
            inner: f,
            _marker: marker::PhantomData,
        }
    }
}

// This is just a `sink.with` to transform a multiplex frame to our own frame,
// but we can name it.
impl<F, T, U> Sink for MyTransport<F, T, U>
    where F: Sink<SinkItem = (RequestId, U), SinkError = io::Error>,
{
    type SinkItem = multiplex::Frame<U, (), io::Error>;
    type SinkError = io::Error;

    fn start_send(&mut self, request: Self::SinkItem)
                  -> StartSend<Self::SinkItem, Self::SinkError> {
        match request {
            multiplex::Frame::Message {
                message,
                id,
                body,
                solo,
            } => {
                if !body && !solo {
                    match try!(self.inner.start_send((id, message))) {
                        AsyncSink::Ready => return Ok(AsyncSink::Ready),
                        AsyncSink::NotReady((id, msg)) => {
                            let msg = multiplex::Frame::Message {
                                message: msg,
                                id: id,
                                body: false,
                                solo: false,
                            };
                            return Ok(AsyncSink::NotReady(msg))
                        }
                    }
                }
            }
            _ => {}
        }
        Err(io::Error::new(io::ErrorKind::Other, "no support for streaming"))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete()
    }
}

// This is just a `stream.map` to transform our frames into multiplex frames
// but we can name it.
impl<F, T, U> Stream for MyTransport<F, T, U>
    where F: Stream<Item = (RequestId, T), Error = io::Error>,
{
    type Item = multiplex::Frame<T, (), io::Error>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (id, msg) = match try_ready!(self.inner.poll()) {
            Some(msg) => msg,
            None => return Ok(None.into()),
        };
        Ok(Some(multiplex::Frame::Message {
            message: msg,
            body: false,
            solo: false,
            id: id,
        }).into())
    }
}

/// An "easy" multiplexed server.
///
/// This struct is an implementation of a server which takes a `FramedIo` (`T`)
/// and dispatches all requests to a service (`S`) to be handled. Internally
/// requests are multiplexed and dispatched/written appropriately over time.
///
/// Note that no streaming request/response bodies are supported with this
/// server to help simplify generics and get off the ground running. If
/// streaming bodies are desired then the top level `multiplex::Server` type can
/// be used (which this is built on).
pub struct EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = (RequestId, S::Request), Error = io::Error> +
             Sink<SinkItem = (RequestId, S::Response), SinkError = io::Error>,
{
    inner: multiplex::Server<MyService<S>,
                             MyTransport<T, S::Request, S::Response>,
                             Empty<(), io::Error>,
                             S::Request,
                             S::Response,
                             (),
                             (),
                             io::Error>,
}

impl<S, T> EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = (RequestId, S::Request), Error = io::Error> +
             Sink<SinkItem = (RequestId, S::Response), SinkError = io::Error>,
{
    /// Instantiates a new multiplexed server.
    ///
    /// The returned server implements the `Future` trait and is used to
    /// internally drive forward all requests for this given transport. The
    /// `transport` provided is an instance of `FramedIo` where the requests are
    /// dispatched to the `service` provided to get a response to write.
    pub fn new(service: S, transport: T) -> EasyServer<S, T> {
        EasyServer {
            inner: multiplex::Server::new(MyService(service),
                                          MyTransport::new(transport)),
        }
    }
}

impl<S, T> Future for EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = (RequestId, S::Request), Error = io::Error> +
             Sink<SinkItem = (RequestId, S::Response), SinkError = io::Error>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        self.inner.poll()
    }
}

struct MyService<S>(S);

impl<S: Service> Service for MyService<S> {
    type Request = Message<S::Request, Body<(), io::Error>>;
    type Response = Message<S::Response, Empty<(), S::Error>>;
    type Error = S::Error;
    type Future = MyFuture<S::Future, Empty<(), S::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        match req {
            Message::WithoutBody(msg) => {
                MyFuture(self.0.call(msg), marker::PhantomData)
            }
            Message::WithBody(..) => panic!("bodies not supported"),
        }
    }
}

struct MyFuture<F, T>(F, marker::PhantomData<fn() -> T>);

impl<F: Future, T> Future for MyFuture<F, T> {
    type Item = Message<F::Item, T>;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let item = try_ready!(self.0.poll());
        Ok(Message::WithoutBody(item).into())
    }
}
