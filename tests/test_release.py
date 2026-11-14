"""Exercise the staged runnable JAR and launchers using Python 3 standard libraries."""
from pathlib import Path
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
import urllib.request
import urllib.error
import zipfile

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / 'target/release/omni-connect-mcp'
WINDOWS = os.name == 'nt'

class Child:
    def __init__(self, command, cwd, env):
        self.process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.output = queue.Queue()
        self.lines = []
        self.errors = []
        def read_output():
            for line in self.process.stdout:
                message = json.loads(line.decode('utf-8'))
                assert message['jsonrpc'] == '2.0'
                self.lines.append(message)
                self.output.put(message)
        def read_errors():
            for line in self.process.stderr:
                self.errors.append(line.decode('utf-8', errors='replace'))
        threading.Thread(target=read_output, daemon=True).start()
        threading.Thread(target=read_errors, daemon=True).start()
        self.sequence = 0
    def rpc(self, method, params):
        self.sequence += 1
        self.process.stdin.write((json.dumps(dict(jsonrpc='2.0', id=self.sequence,
                                                  method=method, params=params)) + '\n').encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                message = self.output.get(timeout=1)
            except queue.Empty:
                assert self.process.poll() is None, ''.join(self.errors)
                continue
            if message.get('id') == self.sequence:
                assert 'error' not in message, message
                return message['result']
        raise AssertionError('Timed out: ' + ''.join(self.errors))
    def initialize(self):
        result = self.rpc('initialize', dict(protocolVersion='2025-11-25', capabilities={},
                                             clientInfo=dict(name='release-smoke', version='1')))
        assert result['serverInfo']['name'] == 'omni-connect-mcp'
        self.process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        self.process.stdin.flush()
        assert len(self.rpc('tools/list', {})['tools']) == 8
    def finish(self):
        self.process.stdin.close()
        assert self.process.wait(timeout=30) == 0, ''.join(self.errors)
        assert 'Tomcat started' not in ''.join(self.errors)
    def cleanup(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)

def main():
    assert sorted(p.name for p in RELEASE.iterdir()) == ['omni-connect-mcp.jar']
    with zipfile.ZipFile(RELEASE / 'omni-connect-mcp.jar') as jar:
        manifest = jar.read('META-INF/MANIFEST.MF').decode()
        assert 'Start-Class: xyz.fz.omni.app.OmniConnectApplication' in manifest
        assert not any(p.endswith('application.yml') for p in jar.namelist())
        assert not any('/h2-' in p for p in jar.namelist())
    work = ROOT / 'target/launcher tests with spaces'
    work.mkdir(parents=True, exist_ok=True)
    for source in RELEASE.iterdir():
        if source.is_file():
            shutil.copy2(source, work / source.name)
    for name in ('application.yml', 'omni-connect.cmd', 'omni-connect.ps1',
                 'omni-connect.sh', 'omni-connect-stdio', 'omni-connect-stdio.cmd',
                 'omni-connect-stdio.ps1'):
        shutil.copy2(ROOT / name, work / name)
    env = os.environ.copy()
    env['OMNI_AUTH_TOKEN'] = 'smoke-test-token-47AbCd'
    env['OMNI_TEST_JDK'] = env['JAVA_HOME']
    java = str(Path(env['JAVA_HOME']) / 'bin' / ('java.exe' if WINDOWS else 'java'))
    def lifecycle(action, *args):
        cmd = (['cmd.exe', '/d', '/c', 'call', str(work / 'omni-connect.cmd')] if WINDOWS
               else ['bash', str(work / 'omni-connect.sh')])
        capture = work / ('capture-' + str(time.time_ns()) + '.txt')
        with capture.open('wb') as stream:
            result = subprocess.run([*cmd, action, *args], cwd=ROOT, env=env,
                                    stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT, timeout=90)
        output = capture.read_bytes().decode(errors='replace')
        assert result.returncode == 0, output
        print(action + ': OK', flush=True)
        return output
    children = []
    try:
        started = lifecycle('start', '--server.port=0', '--java-home=OMNI_TEST_JDK')
        port = int(re.search(r'Service port: (\d+)', started).group(1))
        endpoint = 'http://127.0.0.1:' + str(port) + '/mcp'
        try:
            urllib.request.urlopen(endpoint, timeout=5)
            raise AssertionError('HTTP should require authentication')
        except urllib.error.HTTPError as error:
            assert error.code == 401
        payload = dict(jsonrpc='2.0', id=1, method='initialize', params=dict(
            protocolVersion='2025-11-25', capabilities={}, clientInfo=dict(name='smoke', version='1')))
        request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(), headers={
            'Authorization': 'Bearer ' + env['OMNI_AUTH_TOKEN'],
            'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream'})
        with urllib.request.urlopen(request, timeout=10) as response:
            assert b'omni-connect-mcp' in response.read()
        print('HTTP authentication: OK', flush=True)
        state = json.loads((work / 'log/.omni-connect-launcher.json').read_text(encoding='utf-8-sig')) if WINDOWS else None
        if state:
            assert state.get('pid') or state.get('Pid'), state
        direct = [java, '-jar', str(work / 'omni-connect-mcp.jar'),
                  '--spring.config.location=file:' + (work / 'application.yml').as_posix(),
                  '--spring.ai.mcp.server.stdio=true']
        for index in range(3):
            command = direct
            if index == 0:
                command = (['cmd.exe', '/d', '/c', 'call', str(work / 'omni-connect-stdio.cmd')] if WINDOWS
                           else ['sh', str(work / 'omni-connect-stdio')])
            child = Child(command, ROOT, env)
            children.append(child)
        for child in children:
            child.initialize()
            result = child.rpc('tools/call', dict(name='convert_config', arguments=dict(content='answer=42')))
            assert not result.get('isError'), result
        lifecycle('restart', '--server.port=0', '--java-home=' + env['JAVA_HOME'])
        lifecycle('stop')
        for child in children:
            assert child.process.poll() is None
            child.rpc('tools/list', {})
            child.finish()
        assert len(list((work / 'log').glob('omni-connect-stdio-*.log'))) >= 3
        assert (work / 'application.yml').read_bytes() == (ROOT / 'application.yml').read_bytes()
        print('Executable JAR, three stdio children, EOF, isolation and release layout: OK')
    finally:
        for child in children:
            child.cleanup()
        lifecycle('stop')

if __name__ == '__main__':
    main()
