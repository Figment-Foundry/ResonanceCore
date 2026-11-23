"""Shared SQLite and native transport contracts on isolated fixture buses."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'agent-msg'))
import agent_msg
import agent_broadcast as membership
import resonance_service
from agent_session import process_identity
BINARY=Path(os.environ.get('RESONANCE_GO_BIN', ROOT/'go'/('resonance-service.exe' if os.name=='nt' else 'resonance-service')))

class NativeResonance(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);self.bus=self.root/'fixture 空间.db'
        self.environment=dict(os.environ,RESONANCE_RUNTIME_DIR=str(self.root/'run'))
        self.db=agent_msg.open_db(str(self.bus));self.addCleanup(self.db.close)
        self.call('start')
        self.addCleanup(self.stop_service)

    def stop_service(self):
        stopped=self.call('stop',check=False)
        if not isinstance(stopped,dict):
            self.fail(stopped.stdout+stopped.stderr)

    def call(self,op,*args,message=None,check=True):
        p=subprocess.run([str(BINARY),'--bus',str(self.bus),op,*args],input=json.dumps(message) if message is not None else None,
                         env=self.environment,capture_output=True,text=True,timeout=20)
        if check:self.assertEqual(p.returncode,0,p.stderr)
        return json.loads(p.stdout) if p.returncode==0 else p

    def request(self,**message):return self.call('request',message=message)

    def participant(self,recipient):
        session=membership.register(self.db,recipient,str(self.root),'codex')
        membership.renew(self.db,recipient,session,ready=True)
        return session

    def test_python_bus_go_send_python_read_and_go_drain(self):
        source=self.participant('sender');target=self.participant('recipient')
        message=self.call('send','--from','sender','--to','recipient','fixture 空间')
        rows=agent_msg.fetch_unread(self.db,'recipient',session=target)
        self.assertEqual(rows[0]['id'],message['message_id']);self.assertEqual(rows[0]['body'],'fixture 空间')
        inbox=self.call('drain','--as','recipient','--session',target)
        self.assertEqual(inbox['messages'][0]['id'],message['message_id'])
        self.assertEqual(agent_msg.fetch_unread(self.db,'recipient',session=target),[])
        reply=agent_msg.send(self.db,'recipient','sender','python reply','reply',message['message_id'])
        inbox=self.call('drain','--as','sender','--session',source)
        self.assertEqual(inbox['messages'][0]['id'],reply)

    def test_attachment_lookup_scope_and_end_through_python_adapter(self):
        identity=process_identity(os.getpid());self.assertIsNotNone(identity)
        message=dict(op='attach',harness='codex',thread='fixture-thread',project=str(self.root),event='SessionStart',
                     owner=dict(kind='pid',pid=os.getpid(),identity=list(identity)))
        started=self.request(**message)
        env={'RESONANCE_RUNTIME_DIR':str(self.root/'run'),'RESONANCE_SERVICE_BACKEND':'go'}
        from unittest.mock import patch
        with patch.dict(os.environ,env):
            lookup=resonance_service.request(str(self.bus),dict(op='lookup',harness='codex',thread='fixture-thread'),start=False)
        self.assertEqual(lookup['session'],started['session'])
        status=self.call('status');self.assertEqual(len(status['attachments']),1)
        self.assertEqual(self.request(**message)['session'],started['session'])
        message['event']='SessionEnd';self.assertTrue(self.request(**message)['closed'])
        self.assertEqual(self.call('status')['attachments'],[])

    def test_owner_exit_retires_and_replaced_session_cannot_consume(self):
        process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            identity=process_identity(process.pid);self.assertIsNotNone(identity)
            attached=self.request(op='attach',harness='codex',thread='owned-fixture',project=str(self.root),event='SessionStart',
                                  owner=dict(kind='pid',pid=process.pid,identity=list(identity)))
        finally:process.terminate();process.wait(timeout=10)
        deadline=time.monotonic()+6
        while time.monotonic()<deadline:
            if not self.call('status')['attachments']:break
            time.sleep(.2)
        self.assertEqual(self.call('status')['attachments'],[])
        row=self.db.execute('SELECT closed FROM participants WHERE session=?',(attached['session'],)).fetchone()
        self.assertEqual(row['closed'],1)
        self.participant('sender');old=self.participant('recipient')
        sent=agent_msg.send(self.db,'sender','recipient','old incarnation')
        membership.close(self.db,'recipient',old);fresh=self.participant('recipient')
        inbox=self.call('drain','--as','recipient','--session',fresh)
        self.assertEqual(inbox['messages'],[])
        self.assertIsNone(self.db.execute('SELECT read_at FROM messages WHERE id=?',(sent,)).fetchone()['read_at'])

if __name__=='__main__':unittest.main()
