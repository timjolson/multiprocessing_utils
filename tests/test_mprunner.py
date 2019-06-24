import pytest
import time
from mp_utils import MPRunner, MPFileHandler
from multiprocessing import Manager
import logging

testlogger = logging.getLogger('testlogger')
h = MPFileHandler('logs/test_mprunner.log', mode='w')
h.setFormatter(logging.Formatter('%(name)s:%(message)s'))
testlogger.setLevel(logging.DEBUG)
testlogger.addHandler(h)

l = MPRunner.logger
l.addHandler(h)
l.setLevel(logging.DEBUG)

mgr = Manager()
lock = mgr.Lock()


def test_basic_constructor():
    mp = MPRunner()
    # test something


def test_register(capsys):
    testlogger.info("-----------Starting test_register")
    mp = MPRunner()
    p = mp.register(func_no_args)
    assert p._target is func_no_args
    assert p.daemon is False

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'no args\n'

    p = mp.register(func_no_args, False)
    assert p._target is func_no_args
    assert p.daemon is False

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'no args\n'

    p = mp.register(func_no_args, True)
    assert p._target is func_no_args
    assert p.daemon is True

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'no args\n'

    p = mp.register(func_kw_args, False, dict(arg1='arg1', arg2='arg2'))
    # test runs with kwargs
    assert p._target is func_kw_args
    assert p.daemon is False

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'

    p = mp.register(func_args, False, ('arg1', 'arg2'))
    # test runs with args
    assert p._target is func_args
    assert p.daemon is False

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'

    p = mp.register(func_kw_a_args, False, ('arg1', ), dict(arg2='arg2'))
    # test runs with args and kwargs
    assert p._target is func_kw_a_args
    assert p.daemon is False

    capsys.readouterr()  # junk prints
    p.run()
    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'


def test_run(capsys):
    testlogger.info("-----------Starting test_run")
    capsys.readouterr()

    mp = MPRunner()
    mp.register(func_no_args)
    mp.register(func_no_args)
    mp.runSerial()

    time.sleep(.5)
    assert capsys.readouterr().out == "no args\nno args\n"


def test_join():
    testlogger.info("-----------Starting test_join")
    mp = MPRunner()
    mp.register(func_no_args)

    wait = .5
    mp.register(delay, False, (wait,))

    start = time.time()
    mp.start()
    mp.join()
    end = time.time()

    elapsed = end - start
    max = wait * 1.5
    assert elapsed > wait
    assert elapsed < max


def test_register_multiple_dict(capsys):
    testlogger.info("-----------Starting test_register_multiple_dict")

    mp = MPRunner()
    procs = {
        func_no_args:None,
        func_kw_args:dict(arg1='arg1', arg2='arg2'),
        func_kw_a_args:(('arg1',), dict(arg2='arg2')),
        func_args:(True, ('arg1', 'arg2')),
    }
    mp.register_multiple(procs)
    procs = {
        func_no_args: False,
        func_kw_args: (True, dict(arg1='arg1', arg2='arg2')),
        func_kw_a_args: (True, ('arg1',), dict(arg2='arg2')),
        func_args: (False, ('arg1', 'arg2')),
    }
    mp.register_multiple(procs)
    procs = {
        func_no_args: True,
        func_kw_args: (False, dict(arg1='arg1', arg2='arg2')),
        func_kw_a_args: (False, ('arg1',), dict(arg2='arg2')),
    }
    mp.register_multiple(procs)
    mp.register(func_no_args, args=())
    mp.register(func_no_args, args=[])
    mp.register(func_no_args, args={})

    testlogger.info(f"N procs = {len(mp._allprocs)}")

    for i, p in enumerate(mp._allprocs):
        t = mp._allprocs[i]._target
        testlogger.info(i, p)
        assert p._target is t

    capsys.readouterr()  # junk prints
    mp.runSerial()

    out = capsys.readouterr().out.split('\n')
    testlogger.info(out)
    assert out.count('no args') == 6
    assert out.count('arg1') == 8
    assert out.count('arg2') == 8


def test_register_multiple_list(capsys):
    testlogger.info("-----------Starting test_register_multiple_list")

    mp = MPRunner()
    procs = [func_no_args, func_no_args, func_no_args, func_no_args]
    mp.register_multiple(procs)

    testlogger.info(f"N procs = {len(mp._allprocs)}")

    for i, p in enumerate(mp._allprocs):
        t = mp._allprocs[i]._target
        testlogger.info(i, p)
        assert p._target is t

    capsys.readouterr()  # junk prints
    mp.runSerial()

    out = capsys.readouterr().out.split('\n')
    testlogger.info(out)
    assert out.count('no args') == 4


def test_func_no_args_daemon():
    testlogger.info("-----------Starting test_func_no_args_daemon")

    mp = MPRunner(func_no_args)
    assert mp.procs['func_no_args'][0]._target is func_no_args
    assert mp.procs['func_no_args'][0].daemon is False

    mp = MPRunner({func_no_args:(False,)})
    assert mp.procs['func_no_args'][0]._target is func_no_args
    assert mp.procs['func_no_args'][0].daemon is False

    mp = MPRunner({func_no_args:(True,)})
    assert mp.procs['func_no_args'][0]._target is func_no_args
    assert mp.procs['func_no_args'][0].daemon is True


def test_delay_not_daemon():
    wait = 1

    mp = MPRunner({delay:(False,(wait,))})

    start = time.time()
    mp.start()
    mp.join()
    end = time.time()

    elapsed = end - start
    max = wait * 1.2
    assert elapsed > wait
    assert elapsed < max


def test_delay_daemon():
    wait = 1

    mp = MPRunner({delay:(True,(wait,))})

    start = time.time()
    mp.start()
    mp.join(.1)
    end = time.time()

    elapsed = end - start
    max = .2
    assert elapsed < max

    mp.join()
    end = time.time()
    elapsed = end - start
    max = wait * 1.2
    assert elapsed > wait
    assert elapsed < max


def test_func_kw_args(capsys):
    testlogger.info("-----------Starting test_func_kw_args")

    mp = MPRunner({
            func_kw_args:(False, dict(arg1='arg1', arg2='arg2'))
        })
    # test runs with kwargs
    assert mp.procs['func_kw_args'][0]._target is func_kw_args
    assert mp.procs['func_kw_args'][0].daemon is False

    capsys.readouterr()  # junk prints
    mp.runSerial()

    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'


def test_func_kw_a_args(capsys):
    testlogger.info("-----------Starting test_func_kw_a_args")

    mp = MPRunner({
            func_kw_a_args:(False, ('arg1',), dict(arg2='arg2'))
        })
    # test runs with kwargs
    assert mp.procs['func_kw_a_args'][0]._target is func_kw_a_args
    assert mp.procs['func_kw_a_args'][0].daemon is False

    capsys.readouterr()  # junk prints
    mp.runSerial()

    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'


def test_func_args(capsys):
    testlogger.info("-----------Starting test_func_args")

    mp = MPRunner({
            func_args:(False, ['arg1', 'arg2'])
        })
    # test runs with kwargs
    assert mp.procs['func_args'][0]._target is func_args
    assert mp.procs['func_args'][0].daemon is False

    capsys.readouterr()  # junk prints
    mp.runSerial()

    out = capsys.readouterr().out
    assert out == 'arg1\narg2\n'


# TODO: finish creating tests

def test_active_children():
    pass

def test_monitor():
    pass

def test_run_process():
    pass

def test_run_command():
    pass

def test_shutdown():
    pass

def test_loop():
    pass


def func_no_args():
    with lock: print('no args')
    testlogger.info("ran func_no_args")

def func_args(*args):
    for a in args:
        with lock: print(a)
    testlogger.info("ran func_args")

def func_kw_args(arg1=None, arg2=None):
    with lock: print(arg1)
    with lock: print(arg2)
    testlogger.info("ran func_kw_args")

def func_kw_a_args(arg1=None, arg2=None):
    with lock: print(arg1)
    with lock: print(arg2)
    testlogger.info("ran func_kw_a_args")

def delay(wait):
    time.sleep(wait)
    with lock: print(f'waited {wait:.1f}')
    testlogger.info("ran delay")
