# sugar-lint: disable

import os
import sys
import signal
import shutil
import logging
import unittest
from os.path import dirname, join, exists, abspath

import active_document as ad
import restful_document as rd
import sugar_network_server
from active_document import coroutine
from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context
from restful_document.router import Router
from restful_document.subscribe_socket import SubscribeSocket
from local_document import env
from local_document.bus import Server
from sugar_network import client
from sugar_network.bus import Bus

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

        os.environ['SUGAR_LOGGER_LEVEL'] = 'all'
        os.environ['HOME'] = tmpdir
        profile_dir = join(tmpdir, '.sugar', 'default')
        os.makedirs(profile_dir)
        shutil.copy(join(root, 'data', 'owner.key'), profile_dir)
        shutil.copy(join(root, 'data', 'owner.key.pub'), profile_dir)

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
        Bus.connection = None

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile)

        self.server = None
        self.mounts = None

        self.forks = []

    def tearDown(self):
        if self.server is not None:
            self.server.stop()
        while Test.httpd_pids:
            self.httpdown(Test.httpd_pids.keys()[0])
        while self.forks:
            pid = self.forks.pop()
            self.assertEqual(0, self.waitpid(pid))
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        sys.stdout.flush()

    def waitpid(self, pid):
        if pid in self.forks:
            self.forks.remove(pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        try:
            __, status = os.waitpid(pid, 0)
            return os.WEXITSTATUS(status)
        except OSError:
            return 0

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

    def fork(self, cb, *args):
        pid = os.fork()
        if not pid:
            try:
                cb(*args)
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

        volume = ad.SingleFolder(classes)
        httpd = coroutine.WSGIServer(('localhost', port), Router(volume))

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
        try:
            os.waitpid(pid, 0)
        except OSError:
            pass

    def start_server(self, classes=None):

        def server():
            self.server.serve_forever()

        if classes is None:
            classes = [User, Context]
        self.server = Server('local', classes)
        coroutine.spawn(server)
        coroutine.dispatch()
        self.mounts = self.server._mounts

    def start_ipc_and_restful_server(self, classes):
        pid = self.fork(self.restful_server, classes)

        self.start_server(classes)

        def wait_connect(event):
            if event['event'] == 'connect':
                connected.set()

        connected = coroutine.Event()
        Bus('/').connect(wait_connect)
        connected.wait()

        return pid

    def restful_server(self, classes=None):
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

        volume = ad.SingleVolume('remote', classes or [User, Context])
        httpd = coroutine.WSGIServer(('localhost', 8000), rd.Router(volume))
        subscriber = SubscribeSocket(volume)
        try:
            coroutine.joinall([
                coroutine.spawn(httpd.serve_forever),
                coroutine.spawn(subscriber.serve_forever),
                ])
        finally:
            httpd.stop()
            subscriber.stop()
            volume.close()
