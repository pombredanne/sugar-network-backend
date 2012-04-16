# sugar-lint: disable

import os
import sys
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

import dbus.glib
import dbus.mainloop.glib

from active_document import env as _env, index_queue as _index_queue
from active_document import sneakernet as _sneakernet
from active_document import storage as _storage
from active_document import document_class as _document_class


root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)


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

        self._logfile = file(logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=logfile)

        _env.data_root.value = tmpdir
        _env.index_flush_timeout.value = 0
        _env.index_flush_threshold.value = 1
        _env.find_limit.value = 1024
        _env.LAYOUT_VERSION = 1
        _env.principal.user = 'me'
        _sneakernet.next_volume_cb = None
        _document_class._DIFF_PAGE_SIZE = 256

        _index_queue.errnum = 0

        _storage._ensure_path_locker = _FakeLocker()

    def tearDown(self):
        self.assertEqual(0, _index_queue.errnum)
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        _index_queue.close()
        sys.stdout.flush()

    def override(self, mod, name, new_handler):
        self._overriden.append((mod, name, getattr(mod, name)))
        setattr(mod, name, new_handler)

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
            __, status = os.waitpid(pid, 0)
            self.assertEqual(0, os.WEXITSTATUS(status))

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


class _FakeLocker(object):

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        pass
