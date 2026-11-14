"""Opt-in stdio acceptance against topics/groups owned by KafkaBrokerTest."""
import json
import os
from pathlib import Path
import shutil

from test_release import Child, ROOT, RELEASE


def main():
    if os.environ.get('OMNI_KAFKA_ACCEPTANCE') != 'true':
        print('SKIP: OMNI_KAFKA_ACCEPTANCE=true is required')
        return
    topic = os.environ['KAFKA_ACCEPTANCE_TOPIC']
    group = os.environ['KAFKA_ACCEPTANCE_GROUP']
    assert topic.startswith('omni-mcp-acceptance-') and group.startswith('omni-mcp-acceptance-')
    runtime = ROOT / 'target/kafka-stdio'
    runtime.mkdir(parents=True, exist_ok=True)
    shutil.copy2(RELEASE / 'omni-connect-mcp.jar', runtime / 'omni-connect-mcp.jar')
    java = str(Path(os.environ['JAVA_HOME']) / 'bin' / ('java.exe' if os.name == 'nt' else 'java'))
    child = Child([java, '-jar', str(runtime / 'omni-connect-mcp.jar'),
        '--spring.config.location=file:./application.yml', '--spring.ai.mcp.server.stdio=true'], ROOT, os.environ.copy())
    try:
        child.initialize()

        def call(name, **arguments):
            result = child.rpc('tools/call', dict(name=name, arguments=arguments))
            assert not result.get('isError'), result
            return json.loads(result['content'][0]['text'])

        call('kafka_connect', bootstrapServers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
        assert len(child.rpc('tools/list', {})['tools']) == 15
        sent = call('kafka_publish_record', topic=topic, payload='stdio acceptance', key='stdio', partition=0)
        assert sent['outcome'] == 'CONFIRMED'
        read = call('kafka_read_records', topic=topic, partition=0, start='offset', offset=sent['offset'], count=1)
        assert read['records'][0]['payload'] == 'stdio acceptance'
        uncommitted = call('kafka_consume_records', topic=topic, groupId=group, count=1, maxWaitMs=15000)
        assert uncommitted['commitStatus'] == 'NOT_REQUESTED'
        committed = call('kafka_consume_records', topic=topic, groupId=group, count=1, maxWaitMs=15000, commit=True)
        assert committed['commitStatus'] == 'COMMITTED'
        assert committed['records'][0]['offset'] == uncommitted['records'][0]['offset'] == 0
        child.finish()
        print('KAFKA_ACCEPTANCE PASS stdio connect, tool refresh, publish/read, default no commit, explicit commit, EOF')
    finally:
        child.cleanup()


if __name__ == '__main__':
    main()
