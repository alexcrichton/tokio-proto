use {Message, Body, Error};
use super::{multiplex, RequestId, Frame, Multiplex, MultiplexMessage};
use tokio_service::Service;
use futures::{Future, Poll, Async, Sink, Stream};
use std::io;

/// A server `Task` that dispatches `Transport` messages to a `Service` using
/// protocol multiplexing.
pub struct Server<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    inner: Multiplex<Dispatch<S, T, B, M1, M2, B1, B2, E>>,
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
    in_flight: Vec<(RequestId, InFlight<S::Future>)>,
}

enum InFlight<F: Future> {
    Active(F),
    Done(Result<F::Item, F::Error>),
}

/// The total number of requests that can be in flight at once.
const MAX_IN_FLIGHT_REQUESTS: usize = 32;

/*
 *
 * ===== Server =====
 *
 */

impl<S, T, B, M1, M2, B1, B2, E> Server<S, T, B, M1, M2, B1, B2, E>
    where S: Service<Request = Message<M1, Body<B1, E>>,
                     Response = Message<M2, B>,
                     Error = E>,
          B: Stream<Item = B2, Error = E>,
          T: Sink<SinkItem = Frame<M2, B2, E>, SinkError = io::Error> +
             Stream<Item = Frame<M1, B1, E>, Error = io::Error>,
          E: From<Error<E>>,
{
    /// Create a new multiplex `Server` dispatcher with the given service and
    /// transport
    pub fn new(service: S, transport: T) -> Server<S, T, B, M1, M2, B1, B2, E> {
        let dispatch = Dispatch {
            service: service,
            transport: transport,
            in_flight: vec![],
        };

        // Create the multiplexer
        let multiplex = Multiplex::new(dispatch);

        // Return the server task
        Server { inner: multiplex }
    }
}

impl<S, T, B, M1, M2, B1, B2, E> multiplex::Dispatch for Dispatch<S, T, B, M1, M2, B1, B2, E>
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

    fn poll(&mut self) -> Poll<Option<MultiplexMessage<Self::In, B, Self::Error>>, io::Error> {
        trace!("Dispatch::poll");

        let mut idx = None;

        for (i, &mut (request_id, ref mut slot)) in self.in_flight.iter_mut().enumerate() {
            trace!("   --> poll; request_id={:?}", request_id);
            if slot.poll() && idx.is_none() {
                idx = Some(i);
            }
        }

        if let Some(idx) = idx {
            // let (request_id, message) = self.in_flight.remove(idx);
            let (request_id, message) = self.in_flight.remove(idx);
            let message = MultiplexMessage {
                id: request_id,
                message: message.unwrap_done(),
                solo: false,
            };

            Ok(Async::Ready(Some(message)))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn dispatch(&mut self, message: MultiplexMessage<Self::Out, Body<Self::BodyOut, Self::Error>, Self::Error>) -> io::Result<()> {
        assert!(self.poll_ready().is_ready());

        let MultiplexMessage { id, message, solo } = message;

        assert!(!solo);

        if let Ok(request) = message {
            let response = self.service.call(request);
            self.in_flight.push((id, InFlight::Active(response)));
        }

        // TODO: Should the error be handled differently?

        Ok(())
    }

    fn poll_ready(&self) -> Async<()> {
        if self.in_flight.len() < MAX_IN_FLIGHT_REQUESTS {
            Async::Ready(())
        } else {
            Async::NotReady
        }
    }

    fn cancel(&mut self, _request_id: RequestId) -> io::Result<()> {
        // TODO: implement
        Ok(())
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
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        self.inner.poll()
            .map_err(|e| {
                debug!("multiplex server connection hit error; {:?}", e);
                ()
            })
    }
}

/*
 *
 * ===== InFlight =====
 *
 */

impl<F> InFlight<F>
    where F: Future,
{
    // Returns true if done
    fn poll(&mut self) -> bool {
        let res = match *self {
            InFlight::Active(ref mut f) => {
                trace!("   --> polling future");
                match f.poll() {
                    Ok(Async::Ready(e)) => Ok(e),
                    Err(e) => Err(e),
                    Ok(Async::NotReady) => return false,
                }
            }
            _ => return true,
        };

        *self = InFlight::Done(res);
        true
    }

    fn unwrap_done(self) -> Result<F::Item, F::Error> {
        match self {
            InFlight::Done(res) => res,
            _ => panic!("future is not ready"),
        }
    }
}
