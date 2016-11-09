use {Error, Body, Message};
use super::pipeline::{self, Pipeline, PipelineMessage};
use super::Frame;
use client::{self, Client, Receiver};
use futures::{Future, Poll, Async, Stream, Sink};
use futures::sync::oneshot;
use tokio_core::reactor::Handle;
use std::collections::VecDeque;
use std::io;

struct Dispatch<T, M1, M2, B2, B, E>
    where E: From<Error<E>>,
{
    transport: T,
    requests: Receiver<M1, M2, B, Body<B2, E>, E>,
    in_flight: VecDeque<oneshot::Sender<Result<Message<M2, Body<B2, E>>, E>>>,
}

/// Connect to the given `addr` and handle using the given Transport and protocol pipelining.
pub fn connect<T, M1, B1, M2, B2, B, E>(transport: T, handle: &Handle)
    -> Client<M1, M2, B, Body<B2, E>, E>
    where T: Stream<Item = Frame<M2, B2, E>, Error = io::Error> +
             Sink<SinkItem = Frame<M1, B1, E>, SinkError = io::Error> +
             'static,
          E: From<Error<E>> + 'static,
          B: Stream<Item = B1, Error = E> + 'static,
          M1: 'static,
          B1: 'static,
          M2: 'static,
          B2: 'static,
{
    let (client, rx) = client::pair();

    let dispatch = Dispatch {
        transport: transport,
        requests: rx,
        in_flight: VecDeque::with_capacity(32),
    };
    let srv = Pipeline::new(dispatch);

    // Spawn the task
    handle.spawn(srv.map_err(|e| {
        // TODO: where to punt this error to?
        error!("pipeline error: {}", e);
    }));

    // Return the client
    client
}

impl<T, M1, B1, M2, B2, B, E> pipeline::Dispatch for Dispatch<T, M1, M2, B2, B, E>
    where T: Stream<Item = Frame<M2, B2, E>, Error = io::Error> +
             Sink<SinkItem = Frame<M1, B1, E>, SinkError = io::Error> +
             'static,
          E: From<Error<E>> + 'static,
          B: Stream<Item = B1, Error = E>,
          M1: 'static,
          B1: 'static,
          M2: 'static,
          B2: 'static,
          B: 'static,
{
    type In = M1;
    type BodyIn = B1;
    type Out = M2;
    type BodyOut = B2;
    type Error = E;
    type Stream = B;
    type Transport = T;

    fn transport(&mut self) -> &mut Self::Transport {
        &mut self.transport
    }

    fn dispatch(&mut self, response: PipelineMessage<Self::Out, Body<Self::BodyOut, Self::Error>, Self::Error>) -> io::Result<()> {
        if let Some(complete) = self.in_flight.pop_front() {
            complete.complete(response);
        } else {
            return Err(io::Error::new(io::ErrorKind::Other, "request / response mismatch"));
        }

        Ok(())
    }

    fn poll(&mut self) -> Poll<Option<PipelineMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
        trace!("Dispatch::poll");
        // Try to get a new request frame
        match self.requests.poll() {
            Ok(Async::Ready(Some((request, complete)))) => {
                trace!("   --> received request");

                // Track complete handle
                self.in_flight.push_back(complete);

                Ok(Async::Ready(Some(Ok(request))))

            }
            Ok(Async::Ready(None)) => {
                trace!("   --> client dropped");
                Ok(Async::Ready(None))
            }
            Err(e) => {
                trace!("   --> error");
                // An error on receive can only happen when the other half
                // disconnected. In this case, the client needs to be
                // shutdown
                panic!("unimplemented error handling: {:?}", e);
            }
            Ok(Async::NotReady) => {
                trace!("   --> not ready");
                Ok(Async::NotReady)
            }
        }
    }

    fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }
}

impl<T, M1, M2, B2, B, E> Drop for Dispatch<T, M1, M2, B2, B, E>
    where E: From<Error<E>>,
{
    fn drop(&mut self) {
        // Complete any pending requests with an error
        while let Some(complete) = self.in_flight.pop_front() {
            let err = Error::Io(broken_pipe());
            complete.complete(Err(err.into()));
        }
    }
}

fn broken_pipe() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe")
}
