import { BTree } from "@wecandobetter/btree";


for (let degree = 3; degree < 512; degree++) {
    const tree = new BTree(
        24,
        (a, b) => a-b,
        (a) => a
    )
    let avg = 0;
    for (let j = 0; j < 100; j++) {
        const start = Bun.nanoseconds();
        for (let i = 0; i<1000000; i++)
          tree.insert(i);
        const dur = Bun.nanoseconds()-start;
        avg += (1_000_000_000 / (dur / 1_000_000)) / 100;
    }
    console.log(degree, avg);
}