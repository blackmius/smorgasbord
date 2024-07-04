function RLE(data) {
    let result = [];
    for (let i = 0; i < data.length; i++) {
        let counter = 0;
        while (i < data.length-1 && data[i] == data[i+1]) {
            counter++;
            i++;
        }
        result.push(counter);
        result.push(data[i]);
    }
    return result;
}
function RLE2(data) {
    let result = [];
    let prevVal = data[0];
    let counter = 0;
    result.push(data[0]);
    for (let i = 1; i < data.length; i++) {
        const val = data[i];
        counter++;
        if (prevVal == val) continue;
        result.push(counter);
        result.push(val);
        prevVal = val;
        counter = 0;
    }
    result.push(counter+1);
    return result;
}

function RLE3(arr){
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

function delta(arr) {
    let ids = new Array(arr.length);
    ids[0] = arr[0];
    for (let i = 1; i < arr.length; i++)
        ids[i] = arr[i]-arr[i-1];
    return ids;
}

const f = []
for (let i = 0; i < 1_000_000; i++) f.push(Math.floor(Math.random()*100));
let a = f.sort();
let start = +new Date();
for (let i = 0; i < 1_000; i++)
    RLE(a);
console.log(new Date()-start)

start = +new Date();
for (let i = 0; i < 1_000; i++)
    RLE2(a);
console.log(new Date()-start)

start = +new Date();
for (let i = 0; i < 1_000; i++)
    RLE3(a);
console.log(new Date()-start)

console.log('delta:')

start = +new Date();
for (let i = 0; i < 1_000; i++)
delta(a);
console.log(new Date()-start)

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


console.log('flat:')

let j = {"time": +new Date(), "value": 1, "name": "super_duper_metric"};
start = +new Date();
for (let i =0; i<1_000_000; i++)
    splitColumns(j);
console.log(new Date()-start)
