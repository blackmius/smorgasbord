import { Encoder } from 'msgpackr';
import BTree from 'sorted-btree'

function hd(x, dp=1, units=[], thresh) {
  let u = 0;
  const r = 10**dp;
  while (Math.round(Math.abs(x) * r) / r >= thresh && u < units.length - 1) {
    x /= thresh; ++u;
  }
  return x.toFixed(dp) + ' ' + units[u];
}

function hs(bytes, dp=1) {
  return hd(bytes, dp, ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'], 1024);
}

function ht(ns, dp=1) {
  return hd(ns, dp, ['ns', 'us', 'ms', 's'], 1000);
}

function numberWithCommas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function flatEntry(entry, data, path='')
{
  data = data || [];
  for (const key in entry) {
    if (typeof entry[key] == 'object')
      flatEntry(entry[key], data, path+key+'.')
    else data.push([path+key, entry[key]]);
  }
  return data;
}

class Column {
  constructor(chunk) {
    this.chunk = chunk
    this.data = new BTree(undefined, (a, b)=>{
      return chunk.id2Val[a[1]] < chunk.id2Val[b[1]] ? -1 :
        chunk.id2Val[a[1]] > chunk.id2Val[b[1]] ? 1 : 0;
    });
  }
  push(logId, valId) {
    this.data.set([logId, valId]);
  }
  find(val) {
    // simple expression
    // equal, {$gt, $le, $geq, $leq, $...}
    // XXX: take query out of storage
    const result = [];
    console.log(this.data.size)
    this.data.forEachPair((k, v) => {
      console.log(k, v);
    });
    // const buf = typeof val === 'object' ? pack(val) : val;
    // let a;
    // while (a = this.data.pop()) console.log(a)
    // for (const [logId, valId] of this.data.values) {
    //   if (this.chunk.id2Val[valId] === buf)
    //     result.push(logId)
    // }
    return result;
  }
}

const msgpack = new Encoder({useRecords: false});

class Chunk {
  constructor() {
    this.columns = {};
    this.colName2Id = {};
    this.val2Id = new Map(); // iteration is ordered
    this.id2Val = {};
    this.logId = 0;
    this.colId = 0;
    this.valId = 0;
    this.entries = [];
  }
  getVal(val) {
    const buf = typeof val === 'object' ? msgpack.encode(val) : val;
    if (!this.val2Id.has(buf)) {
      let valId = this.valId++;
      this.val2Id.set(buf, valId);
      this.id2Val[valId] = buf;
    }
    return this.val2Id.get(buf);
  }
  add(entry) {
    const logId = this.logId++;
    const newEntry = [];
    for (const [key, val] of flatEntry(entry)) {
      let colId = this.colName2Id[key] ??= this.colId++;
      const valId = this.getVal(val);
      let column = this.columns[colId] ??= new Column(this);
      column.push(logId, valId);
      newEntry.push([logId, valId]);
    }
    this.entries.push(newEntry);
  }
  write(file) {
    let prev = 0
    for (const val in this.val2Id)
    {
      if (!(prev < this.val2Id[val]))
        console.log('FUCK', prev, this.val2Id[val] )
      prev = this.val2Id[val]
    }
  }
  stats() {
    return {

    }
  }
  find(query) {
    // taken mongo query language
    // $or, $and
    const logs = [];
    for (const colName in query) {
      const colId = this.colName2Id[colName];
      const column = this.columns[colId];
      let sublogs = column.find(query[colName]);
    }
    return logs;
  }
}

async function logs() {
  const f = Bun.file('out.json');
  const lines = await f.text();
  const logs = [];
  for (const line of lines.split('\n')) {
    try {
      logs.push(JSON.parse(line));
    } catch(e) {}
  }
  const chunk = new Chunk();
  const start = Bun.nanoseconds();
  for (const log of logs)
    chunk.add(log);
  const dur = Bun.nanoseconds()-start;
  const rps = Math.floor(1_000_000_000/(dur/logs.length));
  console.log(`logs add x${logs.length}: ${ht(dur)} / ${ht(dur/logs.length)} | ${numberWithCommas(rps)} rps`);
  console.log(chunk.stats());
  console.log(await chunk.write());
}

async function metrics() {
  const metrics = ['a', 'b', 'c', 'd'];
  const chunk = new Chunk();
  const start = Bun.nanoseconds();
  const count = 1000000;
  for (let i = 0; i < count; i++)
  {
    chunk.add({
      name: metrics[Math.floor(Math.random()*metrics.length)],
      value: Math.floor(Math.random()*100),
      time: i
    });
  }
  const dur = Bun.nanoseconds()-start;
  const rps = Math.floor(1_000_000_000/(dur/count));
  console.log(`metrics add x${count}: ${ht(dur)} / ${ht(dur/count)} | ${numberWithCommas(rps)} rps`);
  console.log(chunk.stats());
  console.log(await chunk.write());
  const startA = Bun.nanoseconds();
  console.log(chunk.find({name: 'a'}))
  const durA = Bun.nanoseconds()-start;
  console.log(`metrics find: ${ht(durA)}`);
}

await logs();
await metrics();