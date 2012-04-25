# sugar-lint: disable

import os
import sys
import signal
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

import active_document as ad
from local_document import env
import sugar_network_server

root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    def setUp(self):
        self._overriden = []

        global tmpdir
        tmpdir = join(tmproot, '.'.join(self.id().split('.')[1:]))
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        logfile = tmpdir + '.log'
        if exists(logfile):
            os.unlink(logfile)

        ad.data_root.value = tmpdir
        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10
        env.ipc_root.value = join(tmpdir, 'ipc')

        self._logfile = file(logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=logfile)

        self.forks = []

    def tearDown(self):
        while self.forks:
            pid = self.forks.pop()
            os.kill(pid, signal.SIGTERM)
            __, status = os.waitpid(pid, 0)
            self.assertEqual(0, os.WEXITSTATUS(status))
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        sys.stdout.flush()

    def override(self, mod, name, new_handler):
        self._overriden.append((mod, name, getattr(mod, name)))
        setattr(mod, name, new_handler)

    def touch(self, *files):
        for i in files:
            if isinstance(i, str):
                if i.endswith(os.sep):
                    i = i + '.stamp'
                path = i
                if exists(path):
                    content = file(path).read()
                else:
                    content = i
            else:
                path, content = i
                if isinstance(content, list):
                    content = '\n'.join(content)
            path = join(tmpdir, path)

            if not exists(dirname(path)):
                os.makedirs(dirname(path))
            if exists(path):
                os.unlink(path)

            f = file(path, 'w')
            f.write(str(content))
            f.close()

    def fork(self, cb):
        pid = os.fork()
        if not pid:
            try:
                cb()
                result = 0
            except Exception:
                logging.exception('Child failed')
                result = 1
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(0)
        else:
            self.forks.append(pid)
