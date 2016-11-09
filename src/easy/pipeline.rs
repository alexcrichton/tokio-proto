//! An "easy" pipelining module
//!
//! This module is the same as the top-level `pipeline` module in this crate
//! except that it has no support for streaming bodies. This in turn simplifies
//! a number of generics and allows for protocols to easily get off the ground
//! running.
//!
//! The API surface area of this module is the same as the top-level one, so no
//! new functionality is introduced here. In fact all APIs here are built on top
//! of the `pipeline` module itself, they just simplify the generics in play and
//! such.

use std::io;
use std::marker;

use futures::stream::Empty;
use futures::{Future, Poll, Stream, Sink, AsyncSink, StartSend};
use tokio_core::reactor::Handle;
use tokio_service::Service;

use easy::EasyClient;
use pipeline;
use {Message, Body};

/// The "easy" form of connecting a pipelined client.
///
/// This function takes an instance of the `tokio_core::io::FrameIo` trait as
/// the `frames` argument, a `handle` to the event loop to connect on, and then
/// returns the connected client.
///
/// The client returned implements the `Service` trait. This trait
/// implementation allows sending requests to the transport provided and returns
/// futures to the responses. All requests are automatically pipelined and
/// managed internally.
pub fn connect<S>(frames: S, handle: &Handle)
                  -> EasyClient<S::SinkItem, S::Item>
    where S: Stream<Error = io::Error> + Sink<SinkError = io::Error> + 'static,
{
    EasyClient {
        inner: pipeline::connect(MyTransport(frames), handle),
    }
}

struct MyTransport<T>(T);

impl<T: Sink<SinkError = io::Error>> Sink for MyTransport<T> {
    type SinkItem = pipeline::Frame<T::SinkItem, (), io::Error>;
    type SinkError = io::Error;

    fn start_send(&mut self, request: Self::SinkItem)
                  -> StartSend<Self::SinkItem, Self::SinkError> {
        if let pipeline::Frame::Message { message, body } = request {
            if !body {
                match try!(self.0.start_send(message)) {
                    AsyncSink::Ready => return Ok(AsyncSink::Ready),
                    AsyncSink::NotReady(msg) => {
                        let msg = pipeline::Frame::Message { message: msg, body: false };
                        return Ok(AsyncSink::NotReady(msg))
                    }
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::Other, "no support for streaming"))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.poll_complete()
    }
}

impl<T: Stream> Stream for MyTransport<T> {
    type Item = pipeline::Frame<T::Item, (), io::Error>;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let item = try_ready!(self.0.poll());
        Ok(item.map(|msg| {
            pipeline::Frame::Message { message: msg, body: false }
        }).into())
    }
}

/// An "easy" pipelined server.
///
/// This struct is an implementation of a server which takes a `FramedIo` (`T`)
/// and dispatches all requests to a service (`S`) to be handled. Internally
/// requests are pipelined and dispatched/written appropriately over time.
///
/// Note that no streaming request/response bodies are supported with this
/// server to help simplify generics and get off the ground running. If
/// streaming bodies are desired then the top level `pipeline::Server` type can
/// be used (which this is built on).
pub struct EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = S::Request, Error = io::Error> +
             Sink<SinkItem = S::Response, SinkError = io::Error> + 'static,
{
    inner: pipeline::Server<MyService<S>,
                            MyTransport<T>,
                            Empty<(), io::Error>,
                            S::Request,
                            S::Response,
                            (),
                            (),
                            io::Error>,
}

impl<S, T> EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = S::Request, Error = io::Error> +
             Sink<SinkItem = S::Response, SinkError = io::Error> + 'static,
{
    /// Instantiates a new pipelined server.
    ///
    /// The returned server implements the `Future` trait and is used to
    /// internally drive forward all requests for this given transport. The
    /// `transport` provided is an instance of `FramedIo` where the requests are
    /// dispatched to the `service` provided to get a response to write.
    pub fn new(service: S, transport: T) -> EasyServer<S, T> {
        EasyServer {
            inner: pipeline::Server::new(MyService(service), MyTransport(transport)),
        }
    }
}

impl<S, T> Future for EasyServer<S, T>
    where S: Service<Error = io::Error>,
          T: Stream<Item = S::Request, Error = io::Error> +
             Sink<SinkItem = S::Response, SinkError = io::Error> + 'static,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
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
