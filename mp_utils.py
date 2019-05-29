import logging
from multiprocessing import current_process, managers, Process
from subprocess import Popen
import threading
import queue
import sys
import os
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class AcquirerProxy(managers.AcquirerProxy):
    _exposed_ = ('acquire', 'release', '__enter__', '__exit__')


if 'redis' in sys.modules:
    import redis
    class RedisStorage(redis.Redis):
        access = 'client'
        getLock = redis.Redis.lock
        getRLock = redis.Redis.lock

        def start(self):
            return self
        connect = start

        # make proxies
        # getValue =
        # getList =
        # getQueue =
        # getJoinableQueue =
        # getEvent =
        # getSemaphore =
        # getBoundedSemaphore =
        # getCondition =
        # getBarrier =
        # getPool =
        # getDict =
        # getArray =
        # getNamespace =


class MPStorage(managers.SyncManager):
    def __init__(self, *args, **kwargs):
        """
        :param(s): accepts same parameters as multiprocessing.managers.BaseManager
        """
        super().__init__(*args, **kwargs)
        self.access = None
        self._storage = dict()

        self.__class__.register(
            'getQueue',
            lambda k, *a, **kw: self._getitem(k, queue.Queue, *a, **kw)
        )
        self.__class__.register(
            'getJoinableQueue',
            lambda k, *a, **kw: self._getitem(k, queue.Queue, *a, **kw),
        )
        self.__class__.register(
            'getEvent',
            lambda k, *a, **kw: self._getitem(k, threading.Event, *a, **kw),
            proxytype=managers.EventProxy
        )
        self.__class__.register(
            'getLock',
            lambda k, *a, **kw: self._getitem(k, threading.Lock, *a, **kw),
            proxytype=AcquirerProxy
        )
        self.__class__.register(
            'getRLock',
            lambda k, *a, **kw: self._getitem(k, threading.RLock, *a, **kw),
            proxytype=AcquirerProxy
        )
        self.__class__.register(
            'getSemaphore',
            lambda k, *a, **kw: self._getitem(k, threading.Semaphore, *a, **kw),
            proxytype=AcquirerProxy
        )
        self.__class__.register(
            'getBoundedSemaphore',
            lambda k, *a, **kw: self._getitem(k, threading.BoundedSemaphore, *a, **kw),
            proxytype=AcquirerProxy
        )
        self.__class__.register(
            'getCondition',
            lambda k, *a, **kw: self._getitem(k, threading.Condition, *a, **kw),
            proxytype=managers.ConditionProxy
        )
        self.__class__.register(
            'getBarrier',
            lambda k, *a, **kw: self._getitem(k, threading.Barrier, *a, **kw),
            proxytype=managers.BarrierProxy
        )
        self.__class__.register(
            'getPool',
            lambda k, *a, **kw: self._getitem(k, pool.Pool, *a, **kw),
            proxytype=managers.PoolProxy
        )
        self.__class__.register(
            'getList',
            lambda k, *a, **kw: self._getitem(k, list, *a, **kw),
            proxytype=managers.ListProxy
        )
        self.__class__.register(
            'getDict',
            lambda k, *a, **kw: self._getitem(k, dict, *a, **kw),
            proxytype=managers.DictProxy
        )
        self.__class__.register(
            'getValue',
            lambda k, *a, **kw: self._getitem(k, managers.Value, *a, **kw),
            proxytype=managers.ValueProxy
        )
        self.__class__.register(
            'getArray',
            lambda k, *a, **kw: self._getitem(k, managers.Array, *a, **kw),
            proxytype=managers.ArrayProxy
        )
        self.__class__.register(
            'getNamespace',
            lambda k, *a, **kw: self._getitem(k, managers.Namespace, *a, **kw),
            proxytype=managers.NamespaceProxy
        )

        self.logger = logging.getLogger(
            '.'.join([__name__, type(self).__name__]) + ':' + str(self.address)
        )
        self.logger.addHandler(logging.NullHandler())

    def _getitem(self, key, _type, *args, **kwargs):
        if _type not in self._storage.keys():
            self.logger.info(f"Creating dict for type {_type.__name__}")
            self._storage[_type] = dict()
        if key not in self._storage[_type].keys():
            self.logger.info(f"Creating {_type.__name__} for key '{key}'")
            item = _type(*args, **kwargs)
            self._storage[_type][key] = item
        return self._storage[_type][key]

    def start(self, *args, **kwargs):
        back = sys.stderr
        try:
            self.logger.info("Server starting...")
            sys.stderr = os.devnull
            super().start(*args, **kwargs)
            self.access = 'server'
            sys.stderr = back
            self.logger.info("Server started.")
        except EOFError:
            self.logger.warning("Server already running.")
            super().connect()
            self.access = 'client'
            sys.stderr = back
        finally:
            sys.stderr = back
        return self


# TODO: pytest MPRunner
class MPRunner(object):
    """
    # https://pymotw.com/2/multiprocessing/basics.html
    """
    logger = logging.getLogger('.'.join([__name__, 'MPRunner']))
    logger.addHandler(logging.NullHandler())

    def __init__(self, procs=None):
        self.procs = defaultdict(list)
        self._allprocs = []
        self._active = []

        if procs is not None:
            self.register_multiple(procs)

    def register_multiple(self, procs):
        if isinstance(procs, dict):
            for k, v in procs.items():
                if v in [None, False, (), {}, []]:
                    self.register(k)
                    continue

                if isinstance(v, dict):
                    self.register(k, kwargs=v)
                    continue

                if isinstance(v, bool):
                    self.register(k, daemon=v)
                    continue

                v = list(v)
                daemon, args, kwargs = False, None, None
                for item in v:
                    if isinstance(item, bool):
                        daemon = item
                    elif isinstance(item, dict):
                        kwargs = item
                    else:
                        args = item
                self.logger.warning(("register_multiple()", k, daemon, args, kwargs))

                self.register(k, daemon, args, kwargs)
        elif isinstance(procs, (list, tuple)) or hasattr(procs, '__iter__') or hasattr(procs, '__next__'):
            for p in procs:
                self.register(p)
        elif callable(procs) or isinstance(procs, str):
            self.register(procs)
        else:
            raise ValueError(f"Provide dict, not {type(procs)}")

    def process_start_callback(self, proc):
        self.logger.info(f"Starting {'daemon ' if proc.daemon else ''}'{proc}' at pid {proc.pid}")

    def process_end_callback(self, proc):
        self.logger.info(f"Finished process '{proc}' at pid {proc.pid}")

    def register_process(self, process, daemon=None):
        if daemon is not None:
            process.daemon = daemon
        self.procs[process.name].append(process)
        self._allprocs.append(process)
        self.logger.info(f"Registered process '{process.name}'")
        return process

    def register_function(self, func, daemon=False, args=None, kwargs=None):
        name = getattr(func, '__name__', "Function")

        pr = self._make_process(name, func, daemon, args, kwargs)

        self.procs[pr.name].append(pr)
        self._allprocs.append(pr)
        self.logger.info(f"Registered function '{pr.name}' {daemon} {args} {kwargs}")
        return pr

    def register_command(self, cmds, daemon=False, args=None, kwargs=None):
        if not isinstance(cmds, str):
            cmds = [c for c in cmds]
            if not all(isinstance(c, str) for c in cmds):
                raise ValueError(f"Commands must all be strings")
            cmds = ' '.join(cmds)

        pr = self._make_process(cmds, Popen, daemon, args, kwargs)

        self.procs[cmds].append(pr)
        self._allprocs.append(pr)
        self.logger.info(f"Registered command '{pr.name}'")
        return pr

    def _make_process(self, name, func, daemon=False, args=None, kwargs=None):
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs

        if args and kwargs:
            return Process(name=name, daemon=daemon, target=func, args=args, kwargs=kwargs)
        elif args:
            return Process(name=name, daemon=daemon, target=func, args=args)
        elif kwargs:
            return Process(name=name, daemon=daemon, target=func, kwargs=kwargs)
        else:
            return Process(name=name, daemon=daemon, target=func)

    def register(self, process, daemon=False, args=None, kwargs=None):
        if isinstance(process, Process):
            return self.register_process(Process, daemon)
        elif callable(process):
            return self.register_function(process, daemon, args, kwargs)
        elif isinstance(process, (str, list, tuple)) or hasattr(process, '__iter__') or hasattr(process, '__next__'):
            return self.register_command(process, daemon, args, kwargs)
        else:
            raise NotImplementedError(f"Cannot handle {process}")

    def run(self):
        self.logger.info(f"Starting {sum(len(p) for p in self.procs.values())} processes.")
        for name, prs in self.procs.items():
            for pr in prs:
                self.process_start_callback(pr)
                pr.run()
                self.process_end_callback(pr)

    def start(self):
        self.logger.info(f"Starting {sum(len(p) for p in self.procs.values())} processes.")
        for name, prs in self.procs.items():
            for pr in prs:
                self._active.append(pr)
                pr.start()
                self.process_start_callback(pr)

    def shutdown(self, wait_for_daemons=True):
        ps = list(self._allprocs)
        ds = []

        if wait_for_daemons is True:
            for p in ps:
                if p.daemon is True:
                    ds.append(p)
                    ps.remove(p)
            self.logger.info(f"Allowing {len(ds)} daemons to continue.")

        self.logger.info(f"Terminating {len(ps)} processes.")
        for p in ps:
            p.terminate()

        self.monitor()
        self.join()
        self.monitor()
        self.logger.info(f"{type(self).__name__} has shutdown.")

    def join(self, timeout=None):
        self.logger.debug("Joining processes")
        for p in self._allprocs:
            p.join(timeout)
            self.logger.debug(f"joined {p}")

    def active_children(self):
        return [p for p in self._allprocs if p.is_alive()]

    def monitor(self):
        active = self.active_children()
        for was_running in self._active:
            if was_running not in active:
                self._active.remove(was_running)
                self.process_end_callback(was_running)

    def loop(self, ignore=None, ignore_daemons=True):
        if ignore is None:
            ignore = []
        if ignore_daemons is True:
            ignore.extend(p for p in self._allprocs if p.daemon is True)

        self.monitor()

        ps = list(self._active)
        for i in ignore:
            ps.remove(i)

        return len(ps) > 0


if 'redis' in sys.modules:
    import redis
    address = ('localhost', 6379)
    lockserver = redis.Redis(host=address[0], port=address[1])
    lockserver.ping()
    _getlock = lockserver.lock
else:
    address = ('localhost', 12345)
    authkey = b'localhost:12345.__mp_log_handlers'
    lockserver = MPStorage(address, authkey=authkey)
    _getlock = lockserver.getRLock


def _get_stream_identifier(stream):
    def get_name(stm):
        if hasattr(stm, 'buffer'):
            buffer = stm.buffer
            if hasattr(buffer, 'raw'):
                raw = buffer.raw
                if hasattr(raw, 'name'):
                    if isinstance(raw.name, str):
                        return raw.name
            if hasattr(buffer, 'name'):
                if isinstance(buffer.name, (str, int)):
                    return buffer.name
        if hasattr(stm, 'name'):
            if isinstance(stm.name, (str, int)):
                return stm.name
        return None

    key = get_name(stream)
    try: key = (current_process().pid, int(key))
    except: pass

    if isinstance(key, str):
        key = os.path.abspath(key)

    if key is None:
        key = (current_process().pid, str(stream))

    return str(key)


class MPStreamHandler(logging.StreamHandler):
    # logger = logging.getLogger('.'.join([__name__, 'MPStreamHandler']))
    # logger.addHandler(logging.NullHandler())

    def __init__(self, stream=None):
        if stream is None:
            stream = sys.stderr
        self.stream = stream
        logging.Handler.__init__(self)

    def lock_factory(self, identifier):
        return _getlock(identifier)

    def createLock(self):
        if isinstance(lockserver, MPStorage):
            if lockserver._state.value == managers.State.INITIAL:
                lockserver.start()

        key = '.'.join([__name__, type(self).__name__]) + ':' + _get_stream_identifier(self.stream)
        self.lock_name = key
        # self.logger.debug(f"Made key '{key}' for {self.stream}")
        self.lock = self.lock_factory(key)

    def flush(self):
        if self.stream and hasattr(self.stream, "flush"):
            self.stream.flush()

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            with self.lock:
                stream.write(msg)
                stream.write(self.terminator)
                self.flush()
        except Exception:
            self.handleError(record)

    def handle(self, record):
        rv = self.filter(record)
        if rv:
            self.emit(record)
        return rv


class MPFileHandler(MPStreamHandler):
    # logger = logging.getLogger('.'.join([__name__, 'MPFileHandler']))
    # logger.addHandler(logging.NullHandler())

    def __init__(self, filename, mode='a', encoding=None, delay=False):
        filename = os.fspath(filename)

        self.baseFilename = os.path.abspath(filename)
        self.mode = mode
        self.encoding = encoding
        self.delay = delay
        if delay:
            logging.Handler.__init__(self)
            self.stream = None
        else:
            MPStreamHandler.__init__(self, self._open())

        filepath = os.path.dirname(self.baseFilename)
        os.makedirs(filepath, exist_ok=True)

    def _open(self):
        if not os.path.exists(os.path.dirname(self.baseFilename)):
            os.makedirs(os.path.dirname(self.baseFilename))
        return open(self.baseFilename, self.mode, encoding=self.encoding)

    def createLock(self):
        if isinstance(lockserver, MPStorage):
            if lockserver._state.value == managers.State.INITIAL:
                lockserver.start()

        key = '.'.join([__name__, type(self).__name__]) + ':' + self.baseFilename
        self.lock_name = key
        # self.logger.debug(f"Made key '{key}' for {self.stream}")
        self.lock = self.lock_factory(key)

    def emit(self, record):
        try:
            msg = self.format(record)
            with self.lock:
                if self.stream is None:
                    self.stream = self._open()
                stream = self.stream
                stream.write(msg)
                stream.write(self.terminator)
                self.flush()
        except Exception:
            self.handleError(record)


__all__ = ['MPStreamHandler', 'MPFileHandler', 'MPStorage', 'MPRunner']
if 'redis' in sys.modules:
    __all__.append('RedisStorage')
