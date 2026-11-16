import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {submit, JobSpec, Job, Molq, MolqError, bindDestination} from '@molcrafts/molq';

test('Node destination decorator submits yielded JobSpecs and preserves return', async () => {
  const temp = await mkdtemp(join(tmpdir(), 'molq-ts-decorator-'));
  const python = process.env.MOLQ_TEST_PYTHON || resolve('../../.venv/bin/python');
  const options = {registry: join(temp, 'registry.db'), projectRoot: temp, command: [python, '-m', 'molq', 'serve', '--stdio']};
  let local;
  try {
    local = await submit('dev', 'local', options);
    const hello = local(function* (name) {
      const first = yield JobSpec.argv(['echo', name]);
      const second = yield JobSpec.argv(['echo', 'second']);
      return {first, second, name};
    });
    const result = await hello('decorated TypeScript');
    assert.ok(result.first instanceof Job);
    assert.notDeepEqual(result.first.ref, result.second.ref);
    assert.equal(result.name, 'decorated TypeScript');
    assert.equal((await result.first.wait(10, 0.05)).completion.successful, true);
    assert.equal((await result.second.wait(10, 0.05)).completion.successful, true);
    assert.equal((await result.first.logs()).text, 'decorated TypeScript\n');
    const id = result.first.ref.cluster_id;
    await local.close();
    local = await submit('dev', 'local', options);
    assert.equal((await local.definition()).id, id);
  } finally {
    if (local) await local.close();
    await rm(temp, {recursive: true, force: true});
  }
});

test('Decoration is lazy, preserves this and submits each yield once', async () => {
  const calls = [];
  const mq = new Molq({call: async (method, params) => { calls.push({method, params}); return {ref: {native_id: String(calls.length)}}; }, close: async () => {}});
  mq.rpc = async (method, params) => { calls.push({method, params}); return {ref: {native_id: String(calls.length)}}; };
  const local = await bindDestination(mq, 'dev', undefined, {projectRoot: '/project'});
  const object = {name: 'receiver', hello: local(function* () { const job = yield JobSpec.argv(['echo', this.name]); return job; })};
  assert.equal(calls.length, 0);
  await object.hello();
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params.spec.execution.units[0].command.argv, ['echo', 'receiver']);
  assert.equal(await local(function* () { return 42; })(), 42);
  assert.equal(await local(value => value * 2)(3), 6);
  await local.close();
});

test('Unknown submission outcome is preserved even if generator cleanup fails', async () => {
  let calls = 0, closed = 0;
  const failure = new MolqError({kind: 'OUTCOME_UNKNOWN', message: 'disconnected', outcome: 'unknown', context: {}});
  const mq = new Molq({call: async () => {}, close: async () => {}});
  mq.rpc = async () => { calls++; throw failure; };
  const local = await bindDestination(mq, 'dev', undefined, {projectRoot: '/project'});
  const hello = local(function* () {
    try { yield JobSpec.argv(['echo', 'first']); yield JobSpec.argv(['echo', 'never']); }
    finally { closed++; throw new Error('cleanup failed'); }
  });
  await assert.rejects(hello(), error => error === failure);
  assert.equal(calls, 1);
  assert.equal(closed, 1);
});

test('Destination binding does not resend uncertain registration or silently select SSH', async () => {
  const calls = [];
  const failure = new MolqError({kind: 'OUTCOME_UNKNOWN', message: 'disconnected', outcome: 'unknown', context: {}});
  const mq = new Molq({call: async () => {}, close: async () => { calls.push('close'); }});
  mq.rpc = async method => {
    calls.push(method);
    if (method === 'clusters.get') throw new MolqError({kind: 'CLUSTER_NOT_FOUND', message: 'missing', outcome: 'not_applied', context: {}});
    throw failure;
  };
  await assert.rejects(bindDestination(mq, 'dev', 'local'), error => error === failure);
  assert.deepEqual(calls, ['clusters.get', 'clusters.register', 'close']);
  mq.rpc = async () => ({scheduler: 'shell', transport: {kind: 'ssh', alias: 'hpc'}});
  await assert.rejects(bindDestination(mq, 'dev', 'local'), /differs: transport/);
});

test('Raw argv yields are rejected before dispatch', async () => {
  const mq = new Molq({call: async () => { assert.fail('dispatched'); }, close: async () => {}});
  const local = await bindDestination(mq, 'dev', undefined, {projectRoot: '/project'});
  await assert.rejects(local(function* () { yield ['echo', 'raw']; })(), /JobSpec/);
});


test('Async generators are rejected at decoration before dispatch', async () => {
  const mq = new Molq({call: async () => { assert.fail('dispatched'); }, close: async () => {}});
  const local = await bindDestination(mq, 'dev', undefined, {projectRoot: '/project'});
  assert.throws(() => local(async function* () { yield JobSpec.argv(['echo', 'hello']); }), /synchronous/);
});

test('JobSpec-only methods reject raw values before dispatch',async()=>{
  const mq=new Molq({call:async()=>assert.fail('dispatched'),close:async()=>{}});
  for(const method of ['submit','validate','preview']) {
    for(const value of [null,[],{argv:['echo']}]) {
      await assert.rejects(mq.cluster('dev')[method](value),error=>error.kind==='INVALID_INPUT');
    }
  }
});

test('transfer copies through files.transfer from the source cluster', async () => {
  const calls = [];
  const mq = new Molq({
    call: async (method, params) => {
      calls.push({method, params});
      if (method === 'molq.hello') return {registry_id:'registry'};
      return {outcome:'applied'};
    },
    close: async () => {},
  });
  const local = mq.cluster('local');
  await local.transfer('/src/', mq.cluster('hpc'), '/dst/', '-a', '--exclude=build/');
  assert.deepEqual(calls.at(-1), {
    method: 'files.transfer',
    params: {
      sources: [{cluster: 'local', path: '/src/'}],
      destination: {cluster: 'hpc', path: '/dst/'},
      options: ['-a', '--exclude=build/'],
    },
  });
  await assert.rejects(local.transfer('/src/', {name:'hpc'}, '/dst/'), error => error.kind === 'INVALID_INPUT');
});

test('concurrent business requests share a single handshake',async()=>{
  let hellos=0;
  const mq=new Molq({call:async(method)=>{if(method==='molq.hello'){hellos++;await new Promise(resolve=>setTimeout(resolve,10));return {registry_id:"test-registry"};}return [];},close:async()=>{}});
  await Promise.all([mq.clusters.list(),mq.clusters.list(),mq.clusters.list()]);
  assert.equal(hellos,1);
});

test('wait validates finite durations as structured client errors',async()=>{
  const mq=new Molq({call:async()=>assert.fail('dispatched'),close:async()=>{}});
  for(const timeout of [NaN,Infinity,0,-1]) await assert.rejects(mq.jobs([]).wait(timeout),error=>error.kind==='INVALID_INPUT');
});

test('destination schema errors are local before RPC or registration',async()=>{
  for (const [kind,options] of [['invalid',{}],['local',{supports_gpu:true}],['local',{transport:null}],['local',{transport:[]}],['local',{name:'override'}]]) {
    let closed=0;
    const mq=new Molq({call:async()=>assert.fail('dispatched'),close:async()=>{closed++;}});
    await assert.rejects(bindDestination(mq,'dev',kind,options),error=>error.kind==='INVALID_INPUT');
    assert.equal(closed,1);
  }
});


test('Saved references are copied before cancellation can target an allocation', async () => {
  const calls = [];
  const mq = new Molq({call: async () => {}, close: async () => {}});
  mq.rpc = async (method, params) => { calls.push({method, params}); return {}; };
  const ref = {registry_id: 'registry', cluster_id: 'cluster', native_id: 'accepted', incarnation: 'original'};
  const job = mq.job(ref);
  const refs = [ref];
  const collection = mq.jobs(refs);
  ref.native_id = 'another-allocation';
  refs.push({...ref});
  await job.cancel();
  await collection.cancel();
  assert.equal(calls[0].params.ref.native_id, 'accepted');
  assert.equal(calls[1].params.refs.length, 1);
  assert.equal(calls[1].params.refs[0].native_id, 'accepted');
});


test('malformed handshakes fail before business calls', async () => {
  for(const hello of [null, {}, {registry_id:42}, {registry_id:null}]) {
    const methods=[];
    const mq=new Molq({call:async method=>{methods.push(method);return hello;},close:async()=>{}});
    await assert.rejects(mq.clusters.list(),error=>error.kind==='RUNTIME_UNAVAILABLE');
    assert.deepEqual(methods,['molq.hello']);
  }
});
