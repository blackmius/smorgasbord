import { unpack, pack } from 'msgpackr';

const f = Bun.file('node_lum.log.1');
let chunkCount = 0;
const decoder = new TextDecoder();

function humanFileSize(bytes, si=false, dp=1) {
    const thresh = si ? 1000 : 1024;
  
    if (Math.abs(bytes) < thresh) {
      return bytes + ' B';
    }
  
    const units = si 
      ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] 
      : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10**dp;
  
    do {
      bytes /= thresh;
      ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);
  
  
    return bytes.toFixed(dp) + ' ' + units[u];
  }

function splitColumns(entry, data, path='')
{
    data = data || {};
    for (const key in entry)
    {
        if (typeof entry[key] == 'object')
            splitColumns(entry[key], data, path+key+'.')
        else data[path+key] = entry[key];
    }
    return data;
}

function parseCommon(line) {
    if (line[0] != 'C' || Number.isNaN(+line[1]))
        return null;
    let [worker, d1, d2, level, ...msg] = line.split(' ');
    const time = new Date(d1+' '+d2);
    level = level.slice(0, -1);
    let data = {}
    if (['GET', 'HEAD', 'POST', 'PUT', 'DELETE'].includes(msg[2]))
    {
        data.http = {
            method: msg[2],
            ip: msg[1],
            duration: msg[0],
            url: msg[3],
            version: msg[4],
            status: +msg[5],
            response_size: +msg[6],
            referer: msg[7],
            ua: msg.slice(8).join(' ')
        }
        msg = 'HTTP REQUEST'
    }
    else if (msg[0] == 'perr')
    {
        data.code = msg[1];
        let json = msg[2];
        if (json[json.length-1] != '}')
        {
            let i = 2
            while (i < msg.length && msg[i][msg[i].length-1] != '}')
            {
                i++;
            }
            json = msg.slice(2, i+1).join(' ');   
        }
        try { Object.assign(data, JSON.parse(json)); } catch(e){}
        msg = 'PERR'
    }
    else {
        msg = msg.join(' ');
    }
    return {worker, time: +time, level, msg, data};
}

let last = ''
let logs = 0;
const out = Bun.file('out.json')
const writer = out.writer();
// const columnsData = {};
// let colId = 0;
// let logId = 0;
// let min, max, avg = 0;
let first = true;
for await (const chunk of f.stream())
{
    const lines = decoder.decode(chunk).split('\n');
    lines[0] = last + lines[0];
    last = lines.pop();
    for (const line of lines)
    {
        const res = parseCommon(line);
        if (!res) continue;
        if (!first) await writer.write('\n');
        await writer.write(JSON.stringify(res));
        first = false;
        // const columns = splitColumns(parseCommon(line));
        // for (const col in columns)
        // {
        //     columnsData[col] ??= {id: colId++, name: col, data: []};
        //     columnsData[col].data.push([columns[col], logId]);
        // };
        // writer.write(pack(Object.keys(columns).map(c=>[columnsData[c].id, 10000])));
        // logs++;
        // logId++;
        // let columnsLength = Object.keys(columns).length;
        // if (min == undefined || min > columnsLength) min = columnsLength;
        // if (max == undefined || max < columnsLength) max = columnsLength;
        // if (columnsLength == 326) console.log(columns)
        // avg += columnsLength;
    }
    // console.log(chunk);
    // break;
};
/*
avg /= logs;
console.log('columns count', 'min:', min, 'max:', max, 'avg:', avg)

function RLE(column) {
  let i = 0;
  let prevValue = {};
  let counter = 0;
  writer.write(pack(column.data[i][0]));
  i++;
  while (i < column.data.length) {
    const [value, id] = column.data[i];
    i++;
    counter++;
    if (prevValue == value) continue;
    writer.write(pack(counter));
    writer.write(pack(value));
    prevValue = value;
    counter = 0;
  }
  writer.write(pack(counter+1));
}

for (let col in columnsData) {
    const column = columnsData[col];
    column.data = column.data.sort();
    writer.write(column.name);
    // check cardinality it may be fully uniq
    // and RLE should not be applied
    RLE(column);
    for (const [value, id] of column.data)
        writer.write(pack(id));
}
console.log('chunkCount:', chunkCount, 'logs:', logs,
    'columns:', Object.keys(columnsData).length);
    */