use {Message, Body, Error};
use super::{pipeline, Frame, Pipeline, PipelineMessage};
use tokio_service::Service;
use futures::{Future, Poll, Async, Stream, Sink};
use std::collections::VecDeque;
use std::io;

// TODO:
//
// - Wait for service readiness
// - Handle request body stream cancellation

/// A server `Task` that dispatches `Transport` messages to a `Service` using
/// protocol pipelining.
pub struct Server<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    inner: Pipeline<Dispatch<S, T, B, M1, M2, B1, B2, E>>,
}

struct Dispatch<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    // The service handling the connection
    service: S,
    transport: T,
    in_flight: VecDeque<InFlight<S::Future>>,
}

enum InFlight<F: Future> {
    Active(F),
    Done(Result<F::Item, F::Error>),
}

impl<S, T, B, M1, M2, B1, B2, E> Server<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    /// Create a new pipeline `Server` dispatcher with the given service and
    /// transport
    pub fn new(service: S, transport: T) -> Server<S, T, B, M1, M2, B1, B2, E> {
        let dispatch = Dispatch {
            service: service,
            transport: transport,
            in_flight: VecDeque::with_capacity(32),
        };

        // Create the pipeline dispatcher
        let pipeline = Pipeline::new(dispatch);

        // Return the server task
        Server { inner: pipeline }
    }
}

impl<S, T, B, M1, M2, B1, B2, E> pipeline::Dispatch for Dispatch<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    type In = M2;
    type BodyIn = B2;
    type Out = M1;
    type BodyOut = B1;
    type Error = E;
    type Stream = B;
    type Transport = T;

    fn transport(&mut self) -> &mut T {
        &mut self.transport
    }

    fn dispatch(&mut self,
                request: PipelineMessage<Self::Out, Body<Self::BodyOut, Self::Error>, Self::Error>)
                -> io::Result<()>
    {
        if let Ok(request) = request {
            let response = self.service.call(request);
            self.in_flight.push_back(InFlight::Active(response));
        }

        // TODO: Should the error be handled differently?

        Ok(())
    }

    fn poll(&mut self) -> Poll<Option<PipelineMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
        for slot in self.in_flight.iter_mut() {
            slot.poll();
        }

        match self.in_flight.front() {
            Some(&InFlight::Done(_)) => {}
            _ => return Ok(Async::NotReady)
        }

        match self.in_flight.pop_front() {
            Some(InFlight::Done(res)) => Ok(Async::Ready(Some(res))),
            _ => panic!(),
        }
    }

    fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }
}

impl<F: Future> InFlight<F> {
    fn poll(&mut self) {
        let res = match *self {
            InFlight::Active(ref mut f) => {
                match f.poll() {
                    Ok(Async::Ready(e)) => Ok(e),
                    Err(e) => Err(e),
                    Ok(Async::NotReady) => return,
                }
            }
            _ => return,
        };
        *self = InFlight::Done(res);
    }
}

impl<S, T, B, M1, M2, B1, B2, E> Future for Server<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        self.inner.poll()
    }
}
