"""Opt-in live RabbitMQ stdio acceptance; isolated vhost is deleted in finally."""
import base64
import json
import os
import shutil
from pathlib import Path
import urllib.request
from urllib.parse import quote
from uuid import uuid4

from test_release import Child, ROOT, RELEASE


def main():
    if os.environ.get('OMNI_RABBITMQ_ACCEPTANCE') != 'true':
        print('SKIP: set OMNI_RABBITMQ_ACCEPTANCE=true to permit isolated live tests')
        return
    base = os.environ['RABBITMQ_MANAGEMENT_URL'].rstrip('/')
    user, password = os.environ['RABBITMQ_USERNAME'], os.environ['RABBITMQ_PASSWORD']
    vhost = 'omni-mcp-acceptance-stdio-' + str(uuid4())
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    auth = 'Basic ' + base64.b64encode((user + ':' + password).encode()).decode()

    def api(method, path, body=None):
        request = urllib.request.Request(base + '/api/' + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': auth, 'Content-Type': 'application/json'})
        with opener.open(request, timeout=10) as response:
            return response.read()

    child = None
    created = False
    try:
        api('PUT', 'vhosts/' + quote(vhost, safe=''), {})
        created = True
        api('PUT', 'permissions/' + quote(vhost, safe='') + '/' + quote(user, safe=''),
            dict(configure='.*', write='.*', read='.*'))
        api('PUT', 'queues/' + quote(vhost, safe='') + '/acceptance', dict(durable=False, auto_delete=False, arguments={}))
        java = str(Path(os.environ['JAVA_HOME']) / 'bin/java.exe')
        runtime = ROOT / 'target/rabbitmq-stdio'
        runtime.mkdir(parents=True, exist_ok=True)
        shutil.copy2(RELEASE / 'omni-connect-mcp.jar', runtime / 'omni-connect-mcp.jar')
        child = Child([java, '-jar', str(runtime / 'omni-connect-mcp.jar'),
            '--spring.config.location=file:./application.yml', '--spring.ai.mcp.server.stdio=true'], ROOT, os.environ.copy())
        child.initialize()

        def call(name, **arguments):
            result = child.rpc('tools/call', dict(name=name, arguments=arguments))
            assert not result.get('isError'), result
            return json.loads(result['content'][0]['text'])

        call('rabbitmq_connect', managementUrl=base, username=user, password=password,
            virtualHost=vhost, amqpAddresses=os.environ['RABBITMQ_AMQP_ADDRESSES'])
        assert len(child.rpc('tools/list', {})['tools']) == 16
        assert call('rabbitmq_publish_message', exchange='', routingKey='acceptance', payload='stdio acceptance')['outcome'] == 'CONFIRMED'
        sample = call('rabbitmq_sample_messages', queue='acceptance', count=1)
        assert sample['requeued'] and sample['messages'][0]['payload'] == 'stdio acceptance'
        consumed = call('rabbitmq_consume_messages', queue='acceptance', count=1, maxWaitMs=1000)
        assert consumed['ackSent'] == 1 and consumed['ackStatus'] == 'SENT'
        child.finish()
        print('PASS stdio: connect, refresh 16 tools, publish confirm, sample requeue, ACK consume, EOF shutdown')
    finally:
        if child:
            child.cleanup()
        if created:
            api('DELETE', 'vhosts/' + quote(vhost, safe=''))
            print('PASS stdio isolated vhost deleted')


if __name__ == '__main__':
    main()
