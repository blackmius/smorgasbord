import std/[json, times, monotimes, tables]
let a = %*{"a": 1, "b": 2, "c": 3}

var start = getMonoTime()
for i in 1..1_000_000:
  for k, v in a.fields.pairs:
    discard
echo getMonoTime()-start

start = getMonoTime()
for i in 1..1_000_000:
  for k, v in a.pairs:
    discard
echo getMonoTime()-start

type A = object
 a: int
 b: int

iterator q1(): A =
  for i in 1..1_000_000:
    yield A(a: i)

iterator q2(): A =
  for i in q1():
    yield i

var b = 0
start = getMonoTime()
for i in q1():
  b += i.a
echo getMonoTime()-start
start = getMonoTime()
for i in q2():
  b += i.a
echo getMonoTime()-start