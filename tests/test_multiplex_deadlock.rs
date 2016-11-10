extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate rand;

#[macro_use]
extern crate log;
extern crate env_logger;

mod support;

use support::multiplex as mux;
use support::FnService;

use tokio_proto::Message;
use futures::{Poll, Stream, Sink, StartSend, AsyncSink};
use futures::stream;

#[test]
fn test_write_requires_flush() {

    // Create a custom Transport middleware that requires a flush before
    // enabling reading

    struct Transport<T: Sink> {
        upstream: T,
        buffer: Option<T::SinkItem>,
    }

    impl<T: Sink> Transport<T> {
        fn new(upstream: T) -> Transport<T> {
            Transport {
                upstream: upstream,
                buffer: None,
            }
        }
    }

    impl<T: Stream + Sink> Stream for Transport<T> {
        type Item = T::Item;
        type Error = T::Error;

        fn poll(&mut self) -> Poll<Option<T::Item>, T::Error> {
            self.upstream.poll()
        }
    }

    impl<T: Sink> Sink for Transport<T> {
        type SinkItem = T::SinkItem;
        type SinkError = T::SinkError;

        fn start_send(&mut self, msg: T::SinkItem)
                      -> StartSend<T::SinkItem, T::SinkError> {
            assert!(self.buffer.is_none());
            self.buffer = Some(msg);
            Ok(AsyncSink::Ready)
        }

        fn poll_complete(&mut self) -> Poll<(), T::SinkError> {
            if self.buffer.is_some() {
                let msg = self.buffer.take().unwrap();
                match try!(self.upstream.start_send(msg)) {
                    AsyncSink::Ready => {}
                    AsyncSink::NotReady(_) => panic!("upstream not ready"),
                }
            }

            self.upstream.poll_complete()
        }
    }

    // Define a simple service that just finishes immediately
    let service = FnService::new(|_| {
        let body = vec![0, 1, 2].into_iter().map(Ok);
        let body: mux::BodyBox = Box::new(stream::iter(body));

        let resp = Message::WithBody("goodbye", body);

        futures::finished(resp)
    });

    // Expect a ping pong
    mux::run_with_transport(service, Transport::new, |mock| {
        mock.allow_write();
        mock.send(Some(mux::message(0, "hello")));

        let wr = mock.next_write();
        assert_eq!(wr.request_id(), Some(0));
        assert_eq!(wr.unwrap_msg(), "goodbye");

        for i in 0..3 {
            mock.allow_write();
            let wr = mock.next_write();
            assert_eq!(wr.request_id(), Some(0));
            assert_eq!(wr.unwrap_body(), Some(i));
        }

        mock.allow_write();
        let wr = mock.next_write();
        assert_eq!(wr.request_id(), Some(0));
        assert_eq!(wr.unwrap_body(), None);

        mock.send(None);
        mock.allow_and_assert_drop();
    });
}
