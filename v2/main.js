import { BTree } from "@wecandobetter/btree";
import { pack } from 'msgpackr';

// 1. flatEntry is slow
// one possible solution is to extract it to own service
// it is realated to PARSE part and can be done befor indexing

// 2. inmemory and ondisk is completely different storages
// inmemory after it dumps on disk is never changed and
// uses just sorted lists and buffers
// it is the fastest structures to read (no deletion no replaces allowed)
// so we need to split it from inmemory

// 3. disk storage can not using one big value table
// it should create number only columns which not translate to value tables
// valueTable now only to store strings which bigger than 4 bytes
// XXX: think about using numbers and valid at the same column more

const json = await Bun.file('./k8slog_json.json').json()

function flatEntry(entry, data, path='')
{
  data = data || [];
  for (const key in entry) {
    if (typeof entry[key] == 'object')
      flatEntry(entry[key], data, path+key+'.#')
    else data.push([path+key, entry[key]]);
  }
  return data;
}

function hs(bytes, dp=1) {
    // human size
    const thresh = 1024;
    if (Math.abs(bytes) < thresh)
      return bytes + ' B';
    const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10**dp;
    do {
      bytes /= thresh; ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);
    return bytes.toFixed(dp) + ' ' + units[u];
}

const COLUMN_ENTRY_SIZE = 8;
class Column {
    constructor(chunk) {
        // BTree<[ValId, LogId]>
        this.index = new BTree(24,
            (a, b)=>{
                a = typeof a === 'number' ? chunk.values[a] : a;
                b = typeof b === 'number' ? chunk.values[b] : b;
                return a.compare(b);
            }, a=>a[0])
        this.direct = new Map();
    }

    add(logId, valId) {
        this.index.insert([valId, logId]);
        this.direct.set(logId, valId);
    }
}

class Chunk {
    constructor() {
        this.columns = {};
        this.values = [];
        this.val2id = new Map();
        this.colId = 0;
        this.logId = 0;
        this.valId = 0;

        this.size = 0;
        this.valTableSize = 0;
    }

    getVal(val) {
        // pack is slow
        const buf = pack(val);
        if (typeof val === 'number')
            return buf;
        const key = buf.toString();
        if (!this.val2id.has(key)) {
          let valId = this.valId++;
          this.val2id.set(key, valId);
          this.values.push(buf);
          this.size += buf.length;
          this.valTableSize += buf.length;
        }
        return this.val2id.get(key);
    }

    add(entry) {
        const logId = this.logId++;
        for (const [key, val] of flatEntry(entry)) {
            const valId = this.getVal(val);
            let column = this.columns[this.getVal(key)] ??= new Column(this);
            column.add(logId, valId);
            this.size += COLUMN_ENTRY_SIZE;
        }
    }
}

const chunk = new Chunk();
const start = Bun.nanoseconds();
for (let i = 0; i < 1000000; i++)
    chunk.add(json[i%json.length])
const dur = Bun.nanoseconds()-start;
console.log(1_000_000_000 / (dur / 1_000_000));
console.log(hs(chunk.size))
console.log(hs(chunk.valTableSize))