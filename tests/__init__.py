# sugar-lint: disable

import os
import sys
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

import gobject
import dbus.glib
import dbus.mainloop.glib

from active_document import env as _env


root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None

gobject.threads_init()
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

        _env.root.value = tmpdir
        _env.flush_timeout.value = 0
        _env.flush_threshold.value = 1
        _env.threading.value = False
        _env.LAYOUT_VERSION = 1

    def tearDown(self):
        while self._overriden:
            mod, name, old_handler = self._overriden.pop(0)
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
