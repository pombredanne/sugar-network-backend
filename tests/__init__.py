# sugar-lint: disable

import os
import sys
import signal
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

import gevent

import active_document as ad
import restful_document as rd
from restful_document.router import Router
from restful_document.subscribe_socket import SubscribeSocket
from local_document import env
import sugar_network_server
from sugar_network import client

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

        self.logfile = tmpdir + '.log'
        if exists(self.logfile):
            os.unlink(self.logfile)

        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10
        env.local_root.value = tmpdir
        env.activities_root.value = tmpdir + '/Activities'
        env.api_url.value = 'http://localhost:8000'

        self._logfile = file(self.logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        client._CONNECTION_POOL = 1

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile)

        self.forks = []

    def waitpid(self, pid):
        if pid in self.forks:
            self.forks.remove(pid)
        __, status = os.waitpid(pid, 0)
        return os.WEXITSTATUS(status)

    def tearDown(self):
        while Test.httpd_pids:
            self.httpdown(Test.httpd_pids.keys()[0])
        while self.forks:
            pid = self.forks.pop()
            os.kill(pid, signal.SIGTERM)
            self.assertEqual(0, self.waitpid(pid))
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
            os._exit(result)
        else:
            self.forks.append(pid)
            return pid

    def httpd(self, port, classes):
        if port in Test.httpd_pids:
            self.httpdown(port)

        self.httpd_seqno += 1

        child_pid = os.fork()
        if child_pid:
            time.sleep(1)
            Test.httpd_pids[port] = child_pid
            return

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=tmpdir + '-%s.http.log' % self.httpd_seqno)

        from gevent.wsgi import WSGIServer

        volume = ad.SingleFolder(classes)
        httpd = WSGIServer(('localhost', port), Router(volume))

        try:
            httpd.serve_forever()
        finally:
            httpd.stop()
            volume.close()

    def httpdown(self, port):
        pid = Test.httpd_pids[port]
        del Test.httpd_pids[port]
        os.kill(pid, signal.SIGINT)
        sys.stdout.flush()
        os.waitpid(pid, 0)

    def restful_server(self):
        from gevent.wsgi import WSGIServer
        from sugar_network_server.resources.user import User
        from sugar_network_server.resources.context import Context
        from restful_document import env as _env

        if not exists('remote'):
            os.makedirs('remote')

        logfile = file('remote/log', 'a')
        sys.stdout = sys.stderr = logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG)

        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10

        volume = ad.SingleVolume('remote', [User, Context])
        httpd = WSGIServer(('localhost', 8000), rd.Router(volume))
        subscriber = SubscribeSocket(volume)
        try:
            gevent.joinall([
                gevent.spawn(httpd.serve_forever),
                gevent.spawn(subscriber.serve_forever),
                ])
        finally:
            httpd.stop()
            subscriber.stop()
            volume.close()
