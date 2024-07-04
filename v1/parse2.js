import { unpack, pack } from 'msgpackr';

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

function splitColumns(entry, data, path='')
{
    data = data || [];
    for (const key in entry) {
        if (typeof entry[key] == 'object')
            splitColumns(entry[key], data, path+key+'.')
        else data.push([path+key, entry[key]]);
    }
    return data;
}

function RLE(data) {
    let result = [];
    for (let i = 0; i < data.length; i++) {
        let counter = 0;
        while (i < data.length-1 && data[i] == data[i+1]) {
            counter++;
            i++;
        }
        result.push(data[i]);
        result.push(counter);
    }
    return result;
}

class Chunk {
    constructor() {
        this.colId = 0;
        this.logId = 0;
        this.totalFields = 0;
        this.columns = {};
        this.id2col = {};
        this.entries = [];
    }
    add(entry) {
        const logId = this.logId++;
        const columns = splitColumns(entry);
        const directEntry = [];
        for (const [key, val] of columns) {
            const col = this.columns[key] ??= {id: this.colId++, data: []};
            this.id2col[col.id] = col;
            col.data.push([val, logId]);
            directEntry.push(col.id);
            directEntry.push(val);
        }
        this.entries.push(directEntry);
        if (this.min == undefined || this.min > columns.length) this.min = columns.length;
        if (this.max == undefined || this.max < columns.length) this.max = columns.length;
        this.totalFields += columns.length;
    }
    stats() {
        return {
            min: this.min,
            max: this.max,
            avg: this.totalFields / this.logId,
            cols: this.colId,
            entries: this.logId,
            totalFields: this.totalFields
        }
    }
    async write() {
        const out = Bun.file('out3.bin')
        const writer = out.writer();
        let columnsSize = 0;
        let idsSize = 0;
        const columnSizes = {};
        for (let key in this.columns) {
            const col = this.columns[key];
            const data = col.data.sort((a, b)=>{
                a = a[0];
                b = b[0];
                if (typeof a === 'number' && typeof b === 'number')
                    return a - b;
                if (typeof a === 'string')
                    return a.localeCompare(b);
                if (typeof b === 'string')
                    return b.localeCompare(a);
                return a > b ? 1 : a < b ? -1 : 0;
            });
            let columnSize = 0;
            columnSize += await writer.write(pack(key));
            const rle = RLE(data.map(a=>a[0]));
            columnSize += await writer.write(pack(rle.length < data.length));
            if (rle.length < data.length) {
                col.val2ind = {};
                for (let i = 0; i < rle.length; i += 2)
                    col.val2ind[rle[i]] = i;
                columnSize += await writer.write(pack(rle));
            } else {
                const vals = data.map(a=>a[0])
                col.val2ind = Object.fromEntries(vals.map((a, i)=>[a, i]));
                columnSize += await writer.write(pack(vals));
            }
            // delta-encode+rle ids
            const arr = data.map(a=>a[1]);
            // let ids = arr
            let ids = [arr[0]];
            for (let i = 1; i < arr.length; i++)
                ids.push(arr[i]-arr[i-1]);
            let rleids = RLE(ids);
            if (rleids.length < ids.length)
                ids = rleids;
            let idsData = pack(ids);
            console.log(key, ids.length, data.length, ids.length > data.length, hs(idsData.length));
            idsSize += idsData.length;

            columnSize += await writer.write(idsData);
            columnsSize += columnSize;
            columnSizes[key] = columnSize;
        }
        this.entries.forEach(a=>{
            for (let i = 0; i < a.length; i += 2) {
                let col = this.id2col[a[i]];
                a[i+1] = col.val2ind[a[i+1]];
            }
        })
        const entriesTable = pack(this.entries);
        await writer.write(entriesTable);
        return {
            entriesTableSize: hs(entriesTable.length),
            columnsSize: hs(columnsSize),
            idsSize: hs(idsSize),
            columnSizes: Object.entries(columnSizes).map(([a, b])=>[b, a]).sort((a,b)=>a[0]-b[0]).map(([a, b])=>[b, hs(a)])
        }
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
    for (const log of logs)
        chunk.add(log);
    console.log(chunk.stats());
    console.log(await chunk.write());
}

async function metrics() {
    let i = 0;
    const metrics = ['a', 'b', 'c', 'd'];
    const chunk = new Chunk();
    for (let i = 0; i < 1000000; i++)
    {
        chunk.add({name: metrics[Math.floor(Math.random()*metrics.length)], value: Math.floor(Math.random()*100), time: i});
    }
    console.log(chunk.stats());
    console.log(await chunk.write());
}

await metrics();