import test from 'node:test';
import assert from 'node:assert/strict';
import {StdioWire} from '../dist/node.js';
import {MolqError,isMutation} from '../dist/wire.js';

test('stdio preserves an id:null startup error', async () => {
  const data = {kind:'STATE_SCHEMA_INCOMPATIBLE',message:'Unsupported registry',outcome:'not_applied',context:{}};
  const script = `console.log(${JSON.stringify(JSON.stringify({jsonrpc:'2.0',id:null,error:{code:-32000,message:data.message,data}}))});`;
  const wire = new StdioWire([process.execPath,'-e',script],1);
  try {
    await assert.rejects(wire.call('molq.hello'),error => error instanceof MolqError && error.kind === data.kind);
  } finally { await wire.close(); }
});

test('stdio accepts a 1 MiB JSON frame plus its newline delimiter', async () => {
  const peer = `require('node:readline').createInterface({input:process.stdin}).on('line',line=>{const request=JSON.parse(line);console.log(JSON.stringify({jsonrpc:'2.0',id:request.id,result:true}));});`;
  const wire = new StdioWire([process.execPath,'-e',peer],1);
  const base = JSON.stringify({jsonrpc:'2.0',id:1,method:'test',params:{text:''}});
  try {
    assert.equal(await wire.call('test',{text:'a'.repeat(1048576-Buffer.byteLength(base))}),true);
  } finally { await wire.close(); }
});

test('stdio reports invalid response envelopes as uncertain mutations', async () => {
  const peer = `require('node:readline').createInterface({input:process.stdin}).on('line',line=>{const request=JSON.parse(line);console.log(JSON.stringify({jsonrpc:'invalid',id:request.id,result:true}));});`;
  const wire = new StdioWire([process.execPath,'-e',peer],1);
  try {
    await assert.rejects(wire.call('jobs.submit'),error => error instanceof MolqError && error.kind === 'OUTCOME_UNKNOWN' && error.data.outcome === 'unknown');
  } finally { await wire.close(); }
});

test('invalid outgoing requests are structured and do not start a subprocess', async () => {
  const wire = new StdioWire([process.execPath,'-e','process.exit(99)']);
  await assert.rejects(wire.call('jobs.submit',{text:'a'.repeat(1048576)}), error => error.kind === 'INVALID_INPUT' && error.data.outcome === 'not_applied');
  const cyclic={};cyclic.self=cyclic;
  await assert.rejects(wire.call('jobs.submit',cyclic),error=>error.kind==='INVALID_INPUT');
  assert.equal(wire.process,undefined);
});

test('malformed response envelopes cannot become successful mutation results',async()=>{
  for (const fields of [{}, {result:true,error:{}}, {error:null}, {error:false}, {error:0}, {result:true,method:"events"}, {error:{code:-1,message:'bad',data:{}}}]) {
    const peer=`require('node:readline').createInterface({input:process.stdin}).on('line',line=>{const request=JSON.parse(line);console.log(JSON.stringify({jsonrpc:'2.0',id:request.id,...${JSON.stringify(fields)}}));});`;
    const wire=new StdioWire([process.execPath,'-e',peer],1);
    try { await assert.rejects(wire.call('jobs.submit'),error=>error.kind==='OUTCOME_UNKNOWN'); }
    finally { await wire.close(); }
  }
});

test('HTTP close aborts pending mutations with uncertain outcome',async()=>{
  const {createServer}=await import('node:http');
  const {HttpWire}=await import('../dist/wire.js');
  let received;
  const observed=new Promise(resolve=>received=resolve);
  const server=createServer((request,response)=>{received();});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const wire=new HttpWire(`http://127.0.0.1:${server.address().port}`,'',1);
  try {
    const pending=wire.call('jobs.submit');
    const rejected=assert.rejects(pending,error=>error.kind==='OUTCOME_UNKNOWN');
    await observed;await wire.close();await rejected;
  } finally { server.closeAllConnections();await new Promise(resolve=>server.close(resolve)); }
});

test('HTTP deadline uses seconds and malformed errors stay uncertain',async()=>{
  const {HttpWire}=await import('../dist/wire.js');
  assert.throws(()=>new HttpWire('http://localhost','',NaN),error=>error.kind==='INVALID_INPUT');
  const original=globalThis.fetch;
  globalThis.fetch=async()=>new Response(JSON.stringify({jsonrpc:'2.0',id:1,error:{code:-1,message:'bad',data:{}}}));
  try { await assert.rejects(new HttpWire('http://localhost').call('jobs.submit'),error=>error.kind==='OUTCOME_UNKNOWN'); }
  finally {globalThis.fetch=original;}
});

test('both wires reject nonfinite and dropped JSON values before dispatch',async()=>{
  const {HttpWire}=await import('../dist/wire.js');
  const original=globalThis.fetch;
  globalThis.fetch=async()=>assert.fail('HTTP dispatched');
  try {
    for(const value of [NaN,Infinity,-Infinity,()=>{},Symbol('unsupported'),1n]) {
      const wire=new StdioWire([process.execPath,'-e','process.exit(99)']);
      await assert.rejects(wire.call('jobs.submit',{nested:{value}}),error=>error.kind==='INVALID_INPUT' && error.outcome==='not_applied');
      assert.equal(wire.process,undefined);
      await assert.rejects(new HttpWire('http://localhost').call('jobs.submit',{nested:{value}}),error=>error.kind==='INVALID_INPUT');
    }
  } finally {globalThis.fetch=original;}
});

test('undefined optional fields are omitted and errors serialize as ErrorData',async()=>{
  const {encodeRequest}=await import('../dist/wire.js');
  assert.deepEqual(JSON.parse(encodeRequest(1,'test',{optional:undefined,kept:null})).params,{kept:null});
  const data={kind:'INVALID_INPUT',message:'bad',outcome:'not_applied',context:{field:'timeout'}};
  const error=new MolqError(data);
  assert.equal(error.outcome,'not_applied');assert.deepEqual(error.context,{field:'timeout'});
  assert.deepEqual(JSON.parse(JSON.stringify(error)),data);
});


test('invalid named parameters and overflowing response numbers are rejected', async () => {
  const {HttpWire}=await import('../dist/wire.js');
  const wire=new StdioWire([process.execPath,'-e','process.exit(99)']);
  for(const [method,params] of [['',{}],['jobs.submit',[]],['jobs.submit',null]]) {
    await assert.rejects(wire.call(method,params),error=>error.kind==='INVALID_INPUT');
  }
  assert.equal(wire.process,undefined);
  const original=globalThis.fetch;
  globalThis.fetch=async()=>new Response('{"jsonrpc":"2.0","id":1,"result":1e999}');
  try {await assert.rejects(new HttpWire('http://localhost').call('jobs.submit'),error=>error.kind==='OUTCOME_UNKNOWN');}
  finally {globalThis.fetch=original;}
});

test('connection options are validated before dispatch or local fallback',async()=>{
  const {HttpWire}=await import('../dist/wire.js');
  const {Molq,submit}=await import('../dist/node.js');
  for(const endpoint of ['',null,42,'not a URL','ftp://localhost','http://localhost\n','http://bad host']) {
    assert.throws(()=>new HttpWire(endpoint),error=>error.kind==='INVALID_INPUT');
    await assert.rejects(submit('dev',{endpoint}),error=>error.kind==='INVALID_INPUT');
  }
  for(const token of [null,42,{},'header\nvalue','header\0value']) assert.throws(()=>new HttpWire('http://localhost',token),error=>error.kind==='INVALID_INPUT');
  assert.equal(new HttpWire('https://localhost',undefined).token,'');
  for(const registryId of ['',null,42]) assert.throws(()=>new Molq({call:async()=>{},close:async()=>{}},registryId),error=>error.kind==='INVALID_INPUT');
});

test('job directory purge is a mutation', () => {
  assert.equal(isMutation('jobs.purge'), true);
});
