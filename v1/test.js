let a = [[ "C5", 33313 ], [ "C14", 33314 ], [ "C14", 33315 ], [ "C2", 33316 ], [ "C7",
33317
], [ "C9", 33318 ], [ "C8", 33319 ], [ "C14", 33322 ], [ "C14", 33323 ], [ "C16",
33324
], [ "C12", 33325 ], [ "C11", 33326 ], [ "C15", 33327 ], [ "C4", 33328 ], [ "C1",
33329
], [ "C13", 33330 ], [ "C14", 33331 ], [ "C10", 33332 ], [ "C15", 33333 ], [ "C15",
33334
], [ "C15", 33335 ], [ "C15", 33336 ], [ "C15", 33337 ], [ "C4", 33338 ], [ "C3",
33339
], [ "C5", 33340 ], ["C9", 1]]

a = a.sort()
console.log(a)


// proc RLE[T](arr: openArray[T]): seq[Count[T]] =
//     # XXX: checkout https://github.com/powturbo/Turbo-Run-Length-Encoding
//     result = newSeq[Count[T]]()
//     var count = 1
//     for i in 0..arr.high:
//     if arr[i] != arr[i+1]:
//         result.add Count[T](val: arr[i], count: count)
//         count = 0
//     count += 1

function rle(arr){
    let result = [];
    let count = 1;
    for (let i = 0; i < arr.length; i++) {
        if (arr[i] != arr[i+1]) {
            result.push(arr[i]);
            result.push(count);
            count = 0;
        }
        count++;
    }
    return result;
}

console.log(rle(a.map(q=>q[0])));

const time = performance.now();
var q = []
for (let i = 0; i < 1_000_000; i ++)
    q.push(i);

const dur = performance.now() - time;
console.log(1000/dur*1_000_000)

const time1 = performance.now();
var d = {}
for (let i = 0; i < 1_000_000; i ++)
    d[1_000_000-i] = i;

const dur1 = performance.now() - time;
console.log(1000/dur1*1_000_000)

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

const logs = await Bun.file('k8slog_json.json').json()

const time2 = performance.now();
for (let i = 0; i < 1_000_000; i ++)
    flatEntry(logs[i % logs.length])
const dur2 = performance.now() - time;
console.log(1000/dur2*1_000_000)