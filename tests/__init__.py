# sugar-lint: disable

import os
import sys
import time
import signal
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    httpd_pids = {}

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

        self.httpd_seqno = 0

    def tearDown(self):
        while Test.httpd_pids:
            self.httpdown(Test.httpd_pids.keys()[0])
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
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

    def httpd(self, port):
        if port in Test.httpd_pids:
            self.httpdown(port)

        self.httpd_seqno += 1

        child_pid = os.fork()
        if child_pid:
            time.sleep(0.25)
            Test.httpd_pids[port] = child_pid
            return

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=tmpdir + '-%s.http.log' % self.httpd_seqno)

        from gevent.wsgi import WSGIServer
        import active_document as ad
        import restful_document as rd
        from sugar_network_server import objects
        import sugar_stats_server

        sugar_stats_server.stats_root.value = tmpdir + '/' + 'stats'
        ad.data_root.value = tmpdir + '/' + 'db'
        rd.logdir.value = tmpdir + '/' + 'log'
        rd.rundir.value = tmpdir + '/' + 'run'
        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1

        node = ad.Master(objects.items())
        httpd = WSGIServer(('localhost', port), rd.Router(node))

        try:
            httpd.serve_forever()
        finally:
            httpd.stop()
            node.close()

    def httpdown(self, port):
        pid = Test.httpd_pids[port]
        del Test.httpd_pids[port]
        os.kill(pid, signal.SIGINT)
        sys.stdout.flush()
        os.waitpid(pid, 0)
