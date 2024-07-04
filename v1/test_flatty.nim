import pkg/flatty
import pkg/flatty/hexprint
import std/[times, monotimes]
import std/streams
import pkg/cbor
import pkg/msgpack4nim

template bench(name: string, b: untyped) =
  echo name, ':'
  var start = getMonoTime()
  for i in 1..1000:
    b
  echo getMonoTime() - start
  echo o.len
  echo hexprint(o)

var o: string

bench "flatty":
  o = toFlatty("Hello, world!maaaaaaaaaaaaaaaaa")

bench "msgpack":
  o = pack("Hello, world!maaaaaaaaaaaaaaaaa")

bench "msgpack-number":
  o = pack(259)

bench "cbor":
  o = cbor.encode("Hello, world!maaaaaaaaaaaaaaaaa")
bench "cbor-number":
  o = cbor.encode(259)

bench "stream":
    let s = newStringStream()
    s.write(32)
    s.write("Hello, world!maaaaaaaaaaaaaaaaa")
    o = s.data