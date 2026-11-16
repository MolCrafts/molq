import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {StdioWire} from '../dist/node.js';
import {Molq,methods,MolqError,HttpWire,submit as browserSubmit,JobSpec} from '../dist/index.js';

test('Native TS client uses the same Runtime and schema',async()=>{
  const temp=await mkdtemp(join(tmpdir(),'molq-ts-'));
  const python=process.env.MOLQ_TEST_PYTHON || resolve('../../.venv/bin/python');
  const mq=new Molq(new StdioWire([python,'-m','molq','serve','--stdio','--registry',join(temp,'registry.db')]));
  try{
    const hello=await mq.connect();assert.equal(hello.release,'0.9.0');assert.deepEqual(hello.methods,methods);
    const destination=await mq.clusters.register({name:'local',scheduler:'shell'});
    const job=await destination.submit(JobSpec.argv([python,'-c','print("typescript")']), {projectRoot: temp});
    const result=await job.wait(10,0.05);assert.equal(result.completion.successful,true);
    assert.match((await job.logs()).text,/typescript/);
    await assert.rejects(()=>mq.cluster('missing').submit(JobSpec.argv(['true']), {projectRoot: temp}),error=>error instanceof MolqError && error.kind==='CLUSTER_NOT_FOUND');
  }finally{await mq.close();await rm(temp,{recursive:true,force:true});}
});

test('HTTP peer shares Runtime semantics and structured authentication errors', async()=>{
  const {spawn} = await import('node:child_process');
  const {once} = await import('node:events');
  const {writeFile} = await import('node:fs/promises');
  const {createServer} = await import('node:net');
  const temp=await mkdtemp(join(tmpdir(),'molq-ts-http-'));
  const python=process.env.MOLQ_TEST_PYTHON || resolve('../../.venv/bin/python');
  const token='test-token-long-enough';
  const port=await new Promise((resolvePort,reject)=>{const server=createServer();server.listen(0,'127.0.0.1',()=>{const address=server.address();server.close(()=>resolvePort(address.port));});server.on('error',reject);});
  await writeFile(join(temp,'config.json'), JSON.stringify({serve:{host:'127.0.0.1',port,token}}));
  const child=spawn(python,['-m','molq','serve','--registry',join(temp,'registry.db'),'--config',join(temp,'config.json')],{stdio:['ignore','ignore','inherit']});
  let mq;
  const endpoint='http://127.0.0.1:'+port;
  async function ready(){
    const deadline=Date.now()+5000;
    while(Date.now()<deadline){
      try{const response=await fetch(endpoint+'/rpc',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'molq.hello',params:{}})});if(response.ok)return;}catch{}
      await new Promise(resolveWait=>setTimeout(resolveWait,50));
    }
    throw new Error('HTTP startup deadline');
  }
  try{
    await ready();
    // The streaming client must use the same authenticated /rpc endpoint.
    const stream = new HttpWire(endpoint,token);
    let socket;
    try {
      const subscribed = new Promise((resolveSubscription,reject)=>{
        socket=stream.subscribe(response=>{
          if(response.id===1) resolveSubscription(response);
        });
        socket.addEventListener('error',()=>reject(new Error('WebSocket RPC endpoint unavailable')));
      });
      let timer;
      const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('subscription deadline')),5000);});
      let response;
      try { response=await Promise.race([subscribed,deadline]); } finally { clearTimeout(timer); }
      assert.equal(response.result.resync,true);
    } finally { socket?.close(); }
    const denied=new Molq(new HttpWire(endpoint,'wrong'));
    await assert.rejects(()=>denied.connect(),error=>error instanceof MolqError && error.kind==='AUTH_REQUIRED');
    await denied.close();
    mq=new Molq(new HttpWire(endpoint,token));
    assert.equal((await mq.connect()).release,'0.9.0');
    const destination=await mq.clusters.register({name:'local',scheduler:'shell'});
    const hpc=await browserSubmit('local',{endpoint,token,projectRoot:temp});
    let job;
    try {
      const hello=hpc(function* () { const submitted=yield JobSpec.argv([python,'-c','print("http-peer")']); return submitted; });
      job=await hello();
    } finally { await hpc.close(); }
    assert.equal((await job.wait(10,0.05)).completion.successful,true);
    assert.match((await job.logs()).text,/http-peer/);
    await assert.rejects(()=>mq.rpc('clusters.register',{name:'bad',scheduler:'shell',supports_gpu:true}),error=>error instanceof MolqError && error.kind==='INVALID_INPUT');
  }finally{
    if(mq)await mq.close();
    const exited=once(child,'exit');
    child.kill();
    await exited;
    await rm(temp,{recursive:true,force:true});
  }
});
