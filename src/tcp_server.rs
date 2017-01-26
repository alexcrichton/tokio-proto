use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use BindServer;
use futures::future::{self, Then};
use futures::task::{self, Task};
use futures::{Future, Stream, Poll, Async};
use net2;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::{Core, Handle, Timeout};
use tokio_service::{NewService, Service};

// TODO: Add more options, e.g.:
// - max concurrent requests
// - request timeout
// - read timeout
// - write timeout
// - max idle time
// - max lifetime

/// A builder for TCP servers.
///
/// Setting up a server needs, at minimum:
///
/// - A server protocol implementation
/// - An address
/// - A service to provide
///
/// In addition to those basics, the builder provides some additional
/// configuration, which is expected to grow over time.
///
/// See the crate docs for an example.
pub struct TcpServer<Kind, P> {
    _kind: PhantomData<Kind>,
    proto: Arc<P>,
    threads: usize,
    addr: SocketAddr,
}

/// Instance of a TCP server created through the `TcpServer::bind*` methods.
///
/// This structure is created once a socket address has been bound and a server
/// is ready to start running. An instance supports graceful shutdown through
/// the `run_until` method or otherwise infinite execution through the `run`
/// method.
pub struct TcpServerInstance<Kind, P, S> {
    _kind: PhantomData<Kind>,
    proto: Arc<P>,
    threads: usize,
    core: Core,
    listener: TcpListener,
    new_service: Arc<Fn(&Handle) -> S + Send + Sync>,
    shutdown_timeout: Duration,
}

impl<Kind, P> TcpServer<Kind, P> where
    P: BindServer<Kind, TcpStream> + Send + Sync + 'static
{
    /// Starts building a server for the given protocol and address, with
    /// default configuration.
    ///
    /// Generally, a protocol is implemented *not* by implementing the
    /// `BindServer` trait directly, but instead by implementing one of the
    /// protocol traits:
    ///
    /// - `pipeline::ServerProto`
    /// - `multiplex::ServerProto`
    /// - `streaming::pipeline::ServerProto`
    /// - `streaming::multiplex::ServerProto`
    ///
    /// See the crate documentation for more details on those traits.
    pub fn new(protocol: P, addr: SocketAddr) -> TcpServer<Kind, P> {
        TcpServer {
            _kind: PhantomData,
            proto: Arc::new(protocol),
            threads: 1,
            addr: addr,
        }
    }

    /// Set the address for the server.
    pub fn addr(&mut self, addr: SocketAddr) {
        self.addr = addr;
    }

    /// Set the number of threads running simultaneous event loops (Unix only).
    pub fn threads(&mut self, threads: usize) {
        assert!(threads > 0);
        if cfg!(unix) {
            self.threads = threads;
        }
    }

    /// Start up the server, providing the given service on it.
    ///
    /// This method will block the current thread until the server is shut down.
    pub fn serve<S>(&self, new_service: S) where
        S: NewService + Send + Sync + 'static,
        S::Instance: 'static,
        P::ServiceError: 'static,
        P::ServiceResponse: 'static,
        P::ServiceRequest: 'static,
        S::Request: From<P::ServiceRequest>,
        S::Response: Into<P::ServiceResponse>,
        S::Error: Into<P::ServiceError>,
    {
        self.bind(new_service).expect("failed to bind server")
            .run().expect("error running server")
    }

    /// Bind this server to its configured address, returning the instance.
    ///
    /// This method will create a new I/O reactor, bind a TCP listener to the
    /// specified address, and prepare to service requests with the service
    /// factory provided by `new_service`.
    ///
    /// The returned `TcpServerInstance` can be used to inspect what address was
    /// actually bound, configure further execution options, etc. The returned
    /// server supports two methods, `run` and `run_until` which execute the
    /// server to completion and execute with a shutdown signal, respectively.
    pub fn bind<S>(&self, new_service: S)
                   -> io::Result<TcpServerInstance<Kind, P, Arc<S>>>
        where S: NewService + Send + Sync + 'static,
              S::Instance: 'static,
              P::ServiceError: 'static,
              P::ServiceResponse: 'static,
              P::ServiceRequest: 'static,
              S::Request: From<P::ServiceRequest>,
              S::Response: Into<P::ServiceResponse>,
              S::Error: Into<P::ServiceError>,
    {
        let new_service = Arc::new(new_service);
        self.bind_with_handle(move |_| new_service.clone())
    }

    /// Start up the server, providing the given service on it, and providing
    /// access to the event loop handle.
    ///
    /// The `new_service` argument is a closure that is given an event loop
    /// handle, and produces a value implementing `NewService`. That value is in
    /// turned used to make a new service instance for each incoming connection.
    ///
    /// This method will block the current thread until the server is shut down.
    pub fn with_handle<F, S>(&self, new_service: F)
        where F: Fn(&Handle) -> S + Send + Sync + 'static,
              S: NewService + Send + Sync + 'static,
              S::Instance: 'static,
              P::ServiceError: 'static,
              P::ServiceResponse: 'static,
              P::ServiceRequest: 'static,
              S::Request: From<P::ServiceRequest>,
              S::Response: Into<P::ServiceResponse>,
              S::Error: Into<P::ServiceError>,
    {
        self.bind_with_handle(new_service).expect("failed to bind server")
            .run().expect("error running server")
    }

    /// Bind this server to its configured address, returning the instance.
    ///
    /// This method will create a new I/O reactor, bind a TCP listener to the
    /// specified address, and prepare to service requests with the service
    /// factory provided by `new_service`.
    ///
    /// The returned `TcpServerInstance` can be used to inspect what address was
    /// actually bound, configure further execution options, etc. The returned
    /// server supports two methods, `run` and `run_until` which execute the
    /// server to completion and execute with a shutdown signal, respectively.
    pub fn bind_with_handle<F, S>(&self, new_service: F)
                                  -> io::Result<TcpServerInstance<Kind, P, S>>
        where F: Fn(&Handle) -> S + Send + Sync + 'static,
              S: NewService + Send + Sync + 'static,
              S::Instance: 'static,
              P::ServiceError: 'static,
              P::ServiceResponse: 'static,
              P::ServiceRequest: 'static,
              S::Request: From<P::ServiceRequest>,
              S::Response: Into<P::ServiceResponse>,
              S::Error: Into<P::ServiceError>,
    {
        let core = try!(Core::new());
        let handle = core.handle();
        let listener = try!(listener(&self.addr, self.threads, &handle));
        Ok(TcpServerInstance {
            core: core,
            listener: listener,
            new_service: Arc::new(new_service),
            _kind: PhantomData,
            proto: self.proto.clone(),
            threads: self.threads,
            shutdown_timeout: Duration::new(1, 0),
        })
    }
}

impl<Kind, P, S> TcpServerInstance<Kind, P, S>
    where P: BindServer<Kind, TcpStream> + Send + Sync + 'static,
          S: NewService + Send + Sync + 'static,
          S::Instance: 'static,
          P::ServiceError: 'static,
          P::ServiceResponse: 'static,
          P::ServiceRequest: 'static,
          S::Request: From<P::ServiceRequest>,
          S::Response: Into<P::ServiceResponse>,
          S::Error: Into<P::ServiceError>,
{
    /// Returns the local address that this server is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Returns a handle to the underlying event loop that this server will be
    /// running on.
    ///
    /// Note that for multithreaded servers this will return a handle that the
    /// current thread's event loop will be running.
    pub fn handle(&self) -> Handle {
        self.core.handle()
    }

    /// Configure the amount of time this server will wait for a "graceful
    /// shutdown".
    ///
    /// This is the amount of time after the shutdown signal is received the
    /// server will wait for all pending connections to finish. If the timeout
    /// elapses then the server will be forcibly shut down.
    ///
    /// This defaults to 1s.
    pub fn shutdown_timeout(&mut self, timeout: Duration) {
        self.shutdown_timeout = timeout;
    }

    /// Execute this server infinitely.
    ///
    /// This method does not currently return, but it will return an error if
    /// one occurs.
    pub fn run(self) -> io::Result<()> {
        self.run_until(future::empty())
    }

    /// Execute this server until the given future, `shutdown_signal`, resolves.
    ///
    /// This method, like `run` above, is used to execute this server. The
    /// difference with `run`, however, is that this method allows for shutdown
    /// in a graceful fashion. The future provided is interpreted as a signal to
    /// shut down the server when it resolves.
    ///
    /// This method will block the current thread executing the server.  When
    /// the `shutdown_signal` has resolved then the TCP listener will be unbound
    /// (dropped). The thread will continue to block for a maximum of
    /// `shutdown_timeout` time waiting for active connections to shut down.
    /// Once the `shutdown_timeout` elapses or all active connections are
    /// cleaned out then this method will return.
    pub fn run_until<F>(mut self, shutdown_signal: F) -> io::Result<()>
        where F: Future<Item = (), Error = ()> + Send + 'static,
    {
        let shutdown_signal = shutdown_signal.shared();
        let addr = try!(self.local_addr());

        let threads = (0..self.threads - 1).map(|i| {
            let new_service = self.new_service.clone();
            let shutdown = shutdown_signal.clone();
            let workers = self.threads;
            let timeout = self.shutdown_timeout;
            let proto = self.proto.clone();

            thread::Builder::new().name(format!("worker{}", i)).spawn(move || {
                let mut core = Core::new().unwrap();
                let handle = core.handle();
                let listener = listener(&addr, workers, &handle).unwrap();
                serve(&mut core,
                      &*proto,
                      listener,
                      &*new_service,
                      shutdown,
                      timeout)
            }).unwrap()
        }).collect::<Vec<_>>();

        try!(serve(&mut self.core,
                   &*self.proto,
                   self.listener,
                   &*self.new_service,
                   shutdown_signal,
                   self.shutdown_timeout));

        for thread in threads {
            try!(thread.join().unwrap());
        }

        Ok(())
    }
}

fn serve<P, Kind, F, S, A>(core: &mut Core,
                           binder: &P,
                           listener: TcpListener,
                           new_service: F,
                           shutdown: A,
                           shutdown_timeout: Duration) -> io::Result<()>
    where P: BindServer<Kind, TcpStream>,
          F: Fn(&Handle) -> S,
          S: NewService + Send + Sync,
          S::Instance: 'static,
          P::ServiceError: 'static,
          P::ServiceResponse: 'static,
          P::ServiceRequest: 'static,
          S::Request: From<P::ServiceRequest>,
          S::Response: Into<P::ServiceResponse>,
          S::Error: Into<P::ServiceError>,
          A: Future,
{

    // Mini future to track the number of active services
    let info = Rc::new(RefCell::new(Info {
        active: 0,
        blocker: None,
    }));

    let handle = core.handle();
    let new_service = new_service(&handle);
    let info2 = info.clone();
    let server = listener.incoming().for_each(move |(socket, _)| {
        // Create the service
        info2.borrow_mut().active += 1;
        let service = NotifyService {
            inner: try!(new_service.new_service()),
            info: Rc::downgrade(&info2),
        };

        // Bind it!
        binder.bind_server(&handle, socket, WrapService {
            inner: service,
            _marker: PhantomData,
        });

        Ok(())
    });

    let shutdown = shutdown.then(|_| Ok(()));

    // Main execution of the server. Here we use `select` to wait for either
    // `incoming` or `f` to resolve. We know that `incoming` will never
    // resolve with a success (it's infinite) so we're actually just waiting
    // for an error or for `f`, our shutdown signal.
    //
    // When we get a shutdown signal (`Ok`) then we drop the TCP listener to
    // stop accepting incoming connections.
    match core.run(shutdown.select(server)) {
        Ok(((), _incoming)) => {}
        Err((e, _other)) => return Err(e),
    }

    // Ok we've stopped accepting new connections at this point, but we want
    // to give existing connections a chance to clear themselves out. Wait
    // at most `shutdown_timeout` time before we just return clearing
    // everything out.
    //
    // Our custom `WaitUntilZero` will resolve once all services constructed
    // here have been destroyed.
    let timeout = try!(Timeout::new(shutdown_timeout, &core.handle()));
    let wait = WaitUntilZero { info: info.clone() };
    match core.run(wait.select(timeout)) {
        Ok(_) => Ok(()),
        Err((e, _)) => Err(e),
    }
}

fn listener(addr: &SocketAddr,
            workers: usize,
            handle: &Handle) -> io::Result<TcpListener> {
    let listener = match *addr {
        SocketAddr::V4(_) => try!(net2::TcpBuilder::new_v4()),
        SocketAddr::V6(_) => try!(net2::TcpBuilder::new_v6()),
    };
    try!(configure_tcp(workers, &listener));
    try!(listener.reuse_address(true));
    try!(listener.bind(addr));
    listener.listen(1024).and_then(|l| {
        TcpListener::from_listener(l, addr, handle)
    })
}

#[cfg(unix)]
fn configure_tcp(workers: usize, tcp: &net2::TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    if workers > 1 {
        try!(tcp.reuse_port(true));
    }

    Ok(())
}

#[cfg(windows)]
fn configure_tcp(_workers: usize, _tcp: &net2::TcpBuilder) -> io::Result<()> {
    Ok(())
}

struct NotifyService<S> {
    inner: S,
    info: Weak<RefCell<Info>>,
}

struct WaitUntilZero {
    info: Rc<RefCell<Info>>,
}

struct Info {
    active: usize,
    blocker: Option<Task>,
}

impl<S: Service> Service for NotifyService<S> {
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, message: Self::Request) -> Self::Future {
        self.inner.call(message)
    }
}

impl<S> Drop for NotifyService<S> {
    fn drop(&mut self) {
        let info = match self.info.upgrade() {
            Some(info) => info,
            None => return,
        };
        let mut info = info.borrow_mut();
        info.active -= 1;
        if info.active == 0 {
            if let Some(task) = info.blocker.take() {
                task.unpark();
            }
        }
    }
}

impl Future for WaitUntilZero {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        let mut info = self.info.borrow_mut();
        if info.active == 0 {
            Ok(().into())
        } else {
            info.blocker = Some(task::park());
            Ok(Async::NotReady)
        }
    }
}

struct WrapService<S, Request, Response, Error> {
    inner: S,
    _marker: PhantomData<fn() -> (Request, Response, Error)>,
}

impl<S, Request, Response, Error> Service for WrapService<S, Request, Response, Error>
    where S: Service,
          S::Request: From<Request>,
          S::Response: Into<Response>,
          S::Error: Into<Error>,
{
    type Request = Request;
    type Response = Response;
    type Error = Error;
    type Future = Then<S::Future,
                       Result<Response, Error>,
                       fn(Result<S::Response, S::Error>) -> Result<Response, Error>>;

    fn call(&self, req: Request) -> Self::Future {
        fn change_types<A, B, C, D>(r: Result<A, B>) -> Result<C, D>
            where A: Into<C>,
                  B: Into<D>,
        {
            match r {
                Ok(e) => Ok(e.into()),
                Err(e) => Err(e.into()),
            }
        }

        self.inner.call(S::Request::from(req)).then(change_types)
    }
}
