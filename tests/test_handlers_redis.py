import pytest
import sys
import os
import logging
import multiprocessing as mp
from mp_utils import MPFileHandler, MPStreamHandler

logging.basicConfig(filename='logs/test_handlers_redis.log', level=logging.DEBUG, filemode='w', format="%(name)s:%(message)s")
testlogger = logging.getLogger('testlogger')


def test_lock_identifier():
    testlogger.info("----------------Beginning test_lock_identifier")

    filename = 'logs/test_handlers_redis/test_lock_identifier.log'
    fullpath = os.path.abspath(filename)

    h = MPFileHandler(filename)
    testlogger.info(f"Made lock '{h.lock_name}'")
    assert h.lock_name.endswith(fullpath)

    stm = open(filename, 'w')
    h = MPStreamHandler(stm)
    testlogger.info(f"Made lock '{h.lock_name}'")
    assert h.lock_name.endswith(fullpath)

    h = MPStreamHandler(sys.stdout)
    testlogger.info(f"Made lock '{h.lock_name}'")
    ident = str((mp.current_process().pid, sys.stdout.fileno()))
    assert h.lock_name.endswith(ident)


def test_file_usage():
    testlogger.info("----------------Beginning test_file_usage")

    filename = 'logs/test_handlers_redis/test_file_usage.log'

    testlogger.debug(f"Making MPFileHandler for {filename}")
    h = MPFileHandler(filename, mode='w')
    h.setFormatter(logging.Formatter('%(message)s'))
    h.setLevel(logging.INFO)
    testlogger.addHandler(h)

    testlines = ['error message', 'warning message', 'info message', 'info message after level change', 'debug message after level change']

    testlogger.error(testlines[0])
    testlogger.warning(testlines[1])
    testlogger.info(testlines[2])
    testlogger.debug('debug message that doesnt show up')

    h.setLevel(level=logging.DEBUG)
    testlogger.info(testlines[3])
    testlogger.debug(testlines[4])

    with open(filename, 'r') as f:
        lines = f.readlines()
        assert len(lines) == len(testlines)
        for line, testline in zip(lines, testlines):
            assert line.strip() == testline

    testlogger.removeHandler(h)


def test_stream_usage(capsys):
    testlogger.info("----------------Beginning test_stream_usage")

    testlogger.debug(f"Making MPStreamHandler for sys.stdout:{sys.stdout}")
    h = MPStreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(message)s'))
    testlogger.addHandler(h)

    lines = ['warning message', 'info message one', 'info message two', 'debug message two']

    testlogger.warning(lines[0])
    testlogger.info(lines[1])
    testlogger.log(logging.DEBUG-1, 'subdebug message never to be seen')

    testlogger.setLevel(level=logging.DEBUG)
    testlogger.info(lines[2])
    testlogger.debug(lines[3])

    cap = capsys.readouterr()

    assert cap.out == '\n'.join(lines)+'\n'

    testlogger.removeHandler(h)


def test_multiprocess_file():
    testlogger.info("----------------Beginning test_multiprocess_file")

    filename = 'logs/test_handlers_redis/test_multiprocess_file.log'
    test_l = 200

    def worker(n, file):
        import logging
        from mp_utils import MPFileHandler
        logger = logging.getLogger('mp' + str(n))
        logging.debug(f"Making MPFileHandler for {n} - {file}")
        h = MPFileHandler(file, mode='w')
        h.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(h)

        for _ in range(test_l * 100):
            logger.warning(''.join([str(n)] * test_l))

    ps = [mp.Process(target=worker, args=(i, filename)) for i in range(1,5)]
    [p.start() for p in ps]
    [p.join() for p in ps]

    lines = open(filename, 'r').readlines()
    assert len(lines)>10
    for line in lines:
        line = line.strip()
        check = line[0]
        for c in line:
            assert c == check


def test_multiprocess_stream(capfd):
    testlogger.info("----------------Beginning test_multiprocess_stream")

    filename = 'logs/test_handlers_redis/test_multiprocess_stream.log'
    test_l = 200

    def worker(n, file):
        import logging
        from mp_utils import MPStreamHandler
        logger = logging.getLogger('mp' + str(n))
        logging.debug(f"Making MPStreamHandler {n} - {file}")
        h = MPStreamHandler(file)
        h.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(h)

        for _ in range(test_l * 100):
            logger.warning(''.join([str(n)] * test_l))

    stream = open(filename, 'w')
    ps = [mp.Process(target=worker, args=(i, stream)) for i in range(1,5)]
    [p.start() for p in ps]
    [p.join() for p in ps]

    lines = open(filename, 'r').readlines()
    assert len(lines)>10
    for line in lines:
        line = line.strip()
        check = line[0]
        for c in line:
            assert c == check

    ps = [mp.Process(target=worker, args=(i, sys.stdout)) for i in range(1,5)]
    [p.start() for p in ps]
    [p.join() for p in ps]

    _lines = capfd.readouterr().out
    open(filename+'2', 'w').writelines(_lines)
    _lines = _lines.split()
    assert len(_lines)>10

    lines = []
    for line in _lines:
        line = line.strip()
        while len(line) > test_l:
            lines.append(line[:test_l])
            line = line[test_l:]
        else:
            lines.append(line)

    assert all(len(line) == test_l for line in lines)
    for line in lines:
        check = line[0]
        for c in line:
            assert c == check
