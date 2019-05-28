import pytest
import sys
import logging
import multiprocessing as mp
import redis
from mp_utils import MPFileHandler, MPStreamHandler
logging.basicConfig(filename='logs/test_handlers_redis.log', level=logging.DEBUG, filemode='w')


def test_file_usage():
    filename = 'logs/mplogger_file_handler_redis.log'

    mplogger = logging.getLogger('mplogger-file')
    mplogger.setLevel(logging.INFO)
    logging.debug(f"----------------Making MPFileHandler for {filename}")
    h = MPFileHandler(filename, mode='w')
    h.setFormatter(logging.Formatter('%(name)s:%(message)s'))
    mplogger.addHandler(h)

    mplogger.error('test error')
    mplogger.warning('test warning')
    mplogger.info('test info')
    mplogger.debug('test debug')

    mplogger.setLevel(level=logging.DEBUG)
    mplogger.info('info')
    mplogger.debug('debug')

    with open(filename, 'r') as f:
        lines = f.readlines()
        assert lines[0] == 'mplogger-file:test error\n'
        assert lines[1] == 'mplogger-file:test warning\n'
        assert lines[2] == 'mplogger-file:test info\n'
        assert lines[3] == 'mplogger-file:info\n'
        assert lines[4] == 'mplogger-file:debug\n'


def test_stream_usage(capsys):
    mplogger = logging.getLogger('mplogger-stream')

    logging.debug(f"------------------Making MPStreamHandler for sys.stdout:{sys.stdout}")
    h = MPStreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(name)s:%(message)s'))
    mplogger.addHandler(h)

    mplogger.warning('test warning')
    mplogger.info('test info')

    mplogger.setLevel(level=logging.DEBUG)
    mplogger.info('info')
    mplogger.debug('debug')

    cap = capsys.readouterr()

    assert cap.out == \
           'mplogger-stream:test warning\nmplogger-stream:test info'\
           '\nmplogger-stream:info\nmplogger-stream:debug\n'


def test_multiprocess_file():
    filename = 'logs/mplogger_mp_file_redis.log'
    test_l = 100

    def worker(wait, file):
        import logging
        from mp_utils import MPFileHandler
        logger = logging.getLogger('mp'+str(wait))
        logging.debug(f"---------------Making MPFileHandler for {wait} - {file}")
        h = MPFileHandler(file, mode='w')
        h.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(h)

        for _ in range(test_l * 20):
            logger.warning(''.join([str(wait)] * test_l))

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
    filename = 'logs/mplogger_mp_stream_redis.log'
    test_l = 100

    def worker(wait, file):
        import logging
        from mp_utils import MPStreamHandler
        logger = logging.getLogger('mp'+str(wait))
        logging.debug(f"-------------Making MPStreamHandler for {file}")
        h = MPStreamHandler(file)
        h.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(h)

        for _ in range(test_l * 20):
            logger.warning(''.join([str(wait)]*test_l))

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

    _lines = capfd.readouterr().out.split()
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
