import std/random
import std/algorithm
import std/[times, monotimes]
import std/tables
import std/json
import std/streams
import std/sugar
import std/sequtils
import std/math
import std/strutils
import std/hashes
import pkg/cbor

template bench(name: string, b: untyped) =
  echo name, ':', " (x1000)"
  var start = getMonoTime()
  for i in 1..1_000:
    b
  let dur = getMonoTime() - start
  echo dur
  echo math.floor(1_000_000_000 / (dur.inNanoseconds/1000)), ' ', "rps"

template smallbench(name: string, b: untyped) =
  echo name, ':', " (x1)"
  var start = getMonoTime()
  b
  echo getMonoTime() - start

proc hs(bytes: int, dp=1): string =
  const thresh = 1024
  if bytes < thresh:
    return $(bytes) & " B"
  var b = float(bytes)
  const units = @["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
  var u = -1
  while b > thresh and u < units.high:
    b /= thresh
    u += 1
  return $math.round(b, dp) & ' ' & units[u]

proc toCbor*(js: JsonNode): CborNode {.gcsafe.} =
  # RFC8949 - 6.2.  Converting from JSON to CBOR
  case js.kind
  of JString:
    result = js.str.toCbor
  of JInt:
    result = js.num.toCbor
  of JFloat:
    result = js.fnum.toCbor
  of JBool:
    result = js.bval.toCbor
  of JNull:
    result = CborNode(kind: cborSimple, simple: 22)
  of JObject:
    result = CborNode(kind: cborMap)
    for k, v in js.fields.pairs:
      result[k.toCbor] = v.toCbor
    sort(result)
  of JArray:
    result = CborNode(kind: cborArray, seq: newSeq[CborNode](js.elems.len))
    for i, e in js.elems:
      result.seq[i] = e.toCbor

type Count[T] = object
  val: T
  count: int

proc RLE[T](arr: openArray[T]): seq[Count[T]] =
  # XXX: checkout https://github.com/powturbo/Turbo-Run-Length-Encoding
  result = newSeq[Count[T]]()
  var count = 1
  for i in 0..<arr.high:
    if arr[i] != arr[i+1]:
      result.add Count[T](val: arr[i], count: count)
      count = 0
    count += 1
  if arr.len > 0:
    result.add Count[T](val: arr[^1], count: count)

proc deltaEncode[T](arr: openArray[T]): seq[T] =
  # XXX: SIMD (its second vector is just same with offset 1)
  result = newSeq[T](arr.len)
  if arr.len == 0:
    return
  result[0] = arr[0]
  for i in 1..arr.high:
    result[i] = arr[i]-arr[i-1]

type BloomFilter = object
  data: seq[int64]
  len: int
  n: int
  p: float64
  k: int

proc initBloomFilter(n: int, p: float): BloomFilter =
  const ln2 = ln(2.float32)
  const ln2sqr = ln2*ln2
  let bits = int(-(n.float64*math.ln(p))/ln2sqr)
  result.data = newSeq[int64](bits div 64)
  result.k = int((bits/n)*ln2)
  result.n = n
  result.p = p

proc incl(bf: var BloomFilter, v: int) =
  var q = v.uint64
  var existed = true
  for i in 1..bf.k:
    q = hash(q).uint64
    let bit = q mod (bf.data.len*64).uint64
    let index = bit div 64
    if (bf.data[index] and (1 shl (bit mod 64))) == 0:
        existed = false
    bf.data[index] = bf.data[index] or (1 shl (bit mod 64))
  if not existed:
    bf.len += 1

proc contains(bf: var BloomFilter, v: int): bool =
  var q = v.uint64
  for i in 1..bf.k:
    q = hash(q).uint64
    let bit = q mod (bf.data.len*64).uint64
    let index = bit div 64
    if (bf.data[index] and (1 shl (bit mod 64))) == 0:
      return false
  return true

type
  Id = uint32
  DataEntry = object
    valId: Id
    logId: Id
  Column = ref object
    name: Id
    id: Id
    data: seq[DataEntry] # change to Heap
    log2val: Table[Id, Id]
    offset: int
    logsBloom: BloomFilter
  Field = object
    colId: Id
    valId: Id
  Entry = object
    fields: seq[Field]
  Block = ref object
    logId: Id
    colId: Id
    valId: Id
    columns: seq[Column]
    columnName2Id: OrderedTable[string, Id]
    val2Id: OrderedTable[string, Id]
    # entries: seq[Entry]

proc initBlock(): Block =
  new result
  result.columns = newSeq[Column]()
  result.columnName2Id = initOrderedTable[string, Id]()
  result.val2Id = initOrderedTable[string, Id]()
  # result.entries = newSeq[Entry]()

type
  FlatField = object
    name: string
    val: JsonNode
  FlatEntry = seq[FlatField]

proc flatEntry(arr: var FlatEntry, e: JsonNode, path: string) =
  for k, v in e.fields.pairs:
    if unlikely v.kind == JObject:
      flatEntry(arr, v, path & k & ".")
    else:
      arr.add FlatField(name: k, val: v)

proc flatEntry(e: JsonNode): FlatEntry =
  result = newSeqOfCap[FlatField](e.len)
  flatEntry(result, e, "")

template h(x):untyped = (when defined(lessRan): hashRoMu1(x) else: hashNASAM(x))

proc add(self: Block, entry: JsonNode) =
  assert entry.kind == JObject, "only JObject can be passed"
  let fe = flatEntry(entry)
  # var newEntry = Entry()
  # newEntry.fields = newSeqOfCap[Field](fe.len)
  # XXX: make threadsafe
  let logId = self.logId
  self.logId += 1
  for field in fe:
    # XXX: reuse val2Id (name -> val)
    if not self.columnName2Id.hasKey(field.name):
      self.columnName2Id[field.name] = self.colId
      # XXX: move to own function
      # XXX: replace val2Id with btree
      if not self.val2Id.hasKey(field.name):
        self.val2Id[field.name] = self.valId
        self.valId += 1
      self.columns.add(Column(
        id: self.colId,
        name: self.val2Id[field.name],
        data: newSeq[DataEntry](),
        # logsBloom: initBloomFilter(100, 0.001)
        log2Val: initTable[Id, Id]()
      ))
      self.colId += 1
    let colId = self.columnName2Id[field.name]
    var column = self.columns[colId]
    let val = cbor.encode(field.val.toCbor)
    if not self.val2Id.hasKey(val):
      self.val2Id[val] = self.valId
      self.valId += 1
    let valId = self.val2Id[val]
    column.data.add(DataEntry(logId: logId, valId: valId))
    column.log2val[logId] = valId

    # column.logsBloom.incl(logId.int)
    # if column.logsBloom.len == column.logsBloom.n:
    #   echo "realloc ", column.logsBloom.n
    #   smallbench "realloc bloom":
    #     column.logsBloom = initBloomFilter(column.logsBloom.n*3 div 2, column.logsBloom.p/2)
    #     for t in column.data:
    #       column.logsBloom.incl(t.logId.int)
    # XXX: bloom filter index only after all addings

    # newEntry.fields.add(Field(colId: colId, valId: valId))
  # self.entries.add(newEntry)

type
  Header = object
    valuesOff: int
    columnsOff: int
    entriesOff: int
    valLen: int
    colLen: int
  Encoding = enum
    No
    DeltaRle
  ColumnHeader = object
    name: int
    valLen: int
    logLen: int
    valEncoding: Encoding
    logEncoding: Encoding

proc pack(data: openarray[Id], stream: Stream): tuple[length: int, encoding: Encoding] =
  let d = deltaEncode(data)
  let r = RLE(d)
  if r.len < data.len div 2:
    result.encoding = DeltaRle
    result.length = r.len
  else:
    result.encoding = No
    result.length = data.len
  if result.encoding == DeltaRle:
    for item in r:
      stream.write(item)
  else:
    for item in data:
      stream.write(item)

proc getLog(self: Block, id: Id): Entry = 
  result.fields = newSeq[Field]()
  for col in self.columns:
    if col.log2val.hasKey(id):
      result.fields.add(Field(colId: col.id, valId: col.log2val[id]))

proc dump(self: Block, path: string) =
  var header: Header
  header.valLen = self.val2Id.len
  header.colLen = self.columns.len
  var strm = newFileStream(path, fmWrite)

  for col in self.columns.mitems:
    col.logsBloom = initBloomFilter(col.data.len, 1/col.data.len)
    for t in col.data:
      col.logsBloom.incl(t.logId.int)

  var values = toSeq(self.val2id.values)

  for val in self.val2id.keys():
    strm.write(val)
  header.valuesOff = strm.getPosition()
  var off: int = 0
  for val in self.val2id.keys():
    strm.write(off)
    off += val.len
  var valuesSize = strm.getPosition() 
  var columnsSize = strm.getPosition()
  for col in self.columns.mitems:
    var colHeader: ColumnHeader
    col.offset = strm.getPosition()
    colHeader.name = col.name.int
    var data = col.data
    sort(data, proc (x, y: DataEntry): int =
      cmp(values[x.valId], values[y.valId]))

    strm.writeData(col.logsBloom.data[0].addr, col.logsBloom.data.len)
    strm.writeData(col.logsBloom.n.addr, 32)

    var packed = pack(data.map(t=>t.valId), strm)
    colHeader.valEncoding = packed.encoding
    colHeader.valLen = packed.length

    packed = pack(data.map(t=>t.logId), strm)
    colHeader.logEncoding = packed.encoding
    colHeader.logLen = packed.length
    strm.write(colHeader)

    discard pack(col.data.map(t=>t.valId), strm)
    discard pack(col.data.map(t=>t.logId), strm)

  header.columnsOff = strm.getPosition()
  for col in self.columns:
    strm.write(col.offset)
  columnsSize = strm.getPosition() - columnsSize
  
  # var entriesSize = strm.getPosition()
  # for entry in self.entries.mitems:
  #   entry.offset = strm.getPosition()
  #   strm.writeData(entry.fields[0].addr, sizeof(Field)*entry.fields.len)
  # header.entriesOff = strm.getPosition()
  # var delta = deltaEncode(self.entries.map(x => x.offset))
  # var rle = RLE(delta)
  # for off in rle:
  #   strm.write(off)
  # entriesSize = strm.getPosition() - entriesSize
  
  strm.write(header)
  echo header, ' ', self.valId, ' ', self.logId, ' ', self.colId
  echo "values: ", hs(valuesSize), " columns: ", hs(columnsSize)
    # " entries: ", hs(entriesSize)
  strm.close()

proc rndStr: string =
  for _ in .. 10:
    add(result, char(rand(int('A') .. int('z'))))

proc runBench() =
  var f = newSeq[uint8]()
  for i in 0..<1_000_000:
    f.add rand(100).uint8
  f.sort()

  bench "count":
    var q = 0'u8
    for i in 0..<1_000_000:
      q += f[i]

  bench "RLE":
    discard RLE(f)

  bench "delta":
    discard deltaEncode(f)

  var b = initBlock()
  var j = %*{"time": getTime().toUnix(), "value": 1, "name": "super_duper_metric"}
  bench "block.add":
    b.add(j)
  
  bench "flat":
    discard flatEntry(j)
  
  b = initBlock()
  for i in 1..<1_000_000:
    j["value"].num = i
    j["time"].num = i
    j["name"].str = rndStr()
    b.add(j)
  smallbench "b.dump":
    b.dump("out4.bin")

proc k8bench() =
  let json = readFile("k8slog_json.json")
  let j = parseJson(json)
  let b = initBlock()
  let count = 1_000_000 div j.len
  smallbench "add 1_000_000 k8logs":
    for q in 0..<count:
      for log in j:
        b.add(log)
  var log = newJObject()
  for i in 0..1000:
    log["a" & $i] = JsonNode(kind: JInt, num: i)
  b.add(log)
  smallbench "get some logs":
    echo b.getLog(234)
  smallbench "dump k8logs":
    b.dump("out4.bin")

# runBench()
k8bench()