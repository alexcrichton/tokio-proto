use {Error, Body, Message};
use super::{multiplex, RequestId, Multiplex, MultiplexMessage, Frame};
use client::{self, Client, Receiver};
use futures::{Future, Poll, Async, Sink, Stream};
use futures::sync::oneshot;
use tokio_core::reactor::Handle;
use std::io;
use std::collections::HashMap;

struct Dispatch<T, M1, M2, B2, B, E>
    where E: From<Error<E>>,
{
    transport: T,
    requests: Receiver<M1, M2, B, Body<B2, E>, E>,
    in_flight: HashMap<RequestId, oneshot::Sender<Result<Message<M2, Body<B2, E>>, E>>>,
    next_request_id: u64,
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
        in_flight: HashMap::new(),
        next_request_id: 0,
    };

    let task = Multiplex::new(dispatch)
        .map_err(|err| {
            debug!("multiplex task failed with error; err={:?}", err);
        });

    // Spawn the task
    handle.spawn(task);

    // Return the client
    client
}

impl<T, M1, B1, M2, B2, B, E> multiplex::Dispatch for Dispatch<T, M1, M2, B2, B, E>
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

    fn dispatch(&mut self, message: MultiplexMessage<Self::Out, Body<Self::BodyOut, Self::Error>, Self::Error>) -> io::Result<()> {
        let MultiplexMessage { id, message, solo } = message;

        assert!(!solo);

        if let Some(complete) = self.in_flight.remove(&id) {
            complete.complete(message);
        } else {
            return Err(io::Error::new(io::ErrorKind::Other, "request / response mismatch"));
        }

        Ok(())
    }

    fn poll(&mut self) -> Poll<Option<MultiplexMessage<Self::In, B, Self::Error>>, io::Error> {
        trace!("Dispatch::poll");
        // Try to get a new request frame
        match self.requests.poll() {
            Ok(Async::Ready(Some((request, complete)))) => {
                trace!("   --> received request");

                let request_id = self.next_request_id;
                self.next_request_id += 1;

                trace!("   --> assigning request-id={:?}", request_id);

                // Track complete handle
                self.in_flight.insert(request_id, complete);

                Ok(Async::Ready(Some(MultiplexMessage::new(request_id, request))))

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

    fn poll_ready(&self) -> Async<()> {
        // Not capping the client yet
        Async::Ready(())
    }

    fn cancel(&mut self, _request_id: RequestId) -> io::Result<()> {
        // TODO: implement
        Ok(())
    }
}

impl<T, M1, M2, B2, B, E> Drop for Dispatch<T, M1, M2, B2, B, E>
    where E: From<Error<E>>,
{
    fn drop(&mut self) {
        if !self.in_flight.is_empty() {
            warn!("multiplex client dropping with in-flight exchanges");
        }

        // Complete any pending requests with an error
        for (_, complete) in self.in_flight.drain() {
            let err = Error::Io(broken_pipe());
            complete.complete(Err(err.into()));
        }
    }
}

fn broken_pipe() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe")
}
