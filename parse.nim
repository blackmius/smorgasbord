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
import pkg/cbor

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
  for i in 0..arr.high:
    if arr[i] != arr[i+1]:
      result.add Count[T](val: arr[i], count: count)
      count = 0
    count += 1

proc deltaEncode[T](arr: openArray[T]): seq[T] =
  # XXX: SIMD (its second vector is just same with offset 1)
  result = newSeq[T](arr.len)
  if arr.len == 0:
    return
  result[0] = arr[0]
  for i in 1..arr.high:
    result[i] = arr[i]-arr[i-1]

type
  Id = uint32
  DataEntry = object
    valId: Id
    logId: Id
  Column = ref object
    name: Id
    data: seq[DataEntry] # change to Heap
    offset: int
  Field = object
    colId: Id
    valId: Id
  Entry = object
    offset: int
    fields: seq[Field]
  Block = ref object
    logId: Id
    colId: Id
    valId: Id
    columns: seq[Column]
    columnName2Id: OrderedTable[string, Id]
    val2Id: OrderedTable[string, Id]
    entries: seq[Entry]

proc initBlock(): Block =
  new result
  result.columns = newSeq[Column]()
  result.columnName2Id = initOrderedTable[string, Id]()
  result.val2Id = initOrderedTable[string, Id]()
  result.entries = newSeq[Entry]()

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

proc add(self: Block, entry: JsonNode) =
  assert entry.kind == JObject, "only JObject can be passed"
  let fe = flatEntry(entry)
  var newEntry = Entry()
  newEntry.fields = newSeqOfCap[Field](fe.len)
  # XXX: make threadsafe
  let logId = self.logId
  self.logId += 1
  for field in fe:
    if not self.columnName2Id.hasKey(field.name):
      self.columnName2Id[field.name] = self.colId
      # XXX: move to own function
      if not self.val2Id.hasKey(field.name):
        self.val2Id[field.name] = self.valId
        self.valId += 1
      self.columns.add(Column(name: self.val2Id[field.name], data: newSeq[DataEntry]()))
      self.colId += 1
    let colId = self.columnName2Id[field.name]
    var column = self.columns[colId]
    let val = cbor.encode(field.val.toCbor)
    if not self.val2Id.hasKey(val):
      self.val2Id[val] = self.valId
      self.valId += 1
    let valId = self.val2Id[val]
    column.data.add(DataEntry(logId: logId, valId: valId))
    newEntry.fields.add(Field(colId: colId, valId: valId))
  self.entries.add(newEntry)

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

proc dump(self: Block, path: string) =
  var header: Header
  header.valLen = self.val2Id.len
  header.colLen = self.columns.len
  var strm = newFileStream(path, fmWrite)

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
    let valsDelta = deltaEncode(col.data.map(t => t.valId))
    let valsRle = RLE(valsDelta)
    if valsRle.len < col.data.len div 2:
      colHeader.valEncoding = DeltaRle
      colHeader.valLen = valsRle.len
    else:
      colHeader.valLen = col.data.len
    let logsDelta = deltaEncode(col.data.map(t => t.logId))
    let logsRle = RLE(logsDelta)
    if logsRle.len < col.data.len div 2:
      colHeader.logEncoding = DeltaRle
      colHeader.logLen = logsRle.len
    else:
      colHeader.logLen = col.data.len

    strm.write(colHeader)
    if colHeader.valEncoding == DeltaRle:
      for item in valsRle:
        strm.write(item)
    else:
      for item in col.data:
        strm.write(item.valId)
    
    if colHeader.logEncoding == DeltaRle:
      for item in logsRle:
        strm.write(item)
    else:
      for item in col.data:
        strm.write(item.logId)
  header.columnsOff = strm.getPosition()
  for col in self.columns:
    strm.write(col.offset)
  columnsSize = strm.getPosition() - columnsSize
  
  var entriesSize = strm.getPosition()
  for entry in self.entries.mitems:
    entry.offset = strm.getPosition()
    strm.writeData(entry.fields[0].addr, sizeof(Field)*entry.fields.len)
  header.entriesOff = strm.getPosition()
  var delta = deltaEncode(self.entries.map(x => x.offset))
  var rle = RLE(delta)
  for off in rle:
    strm.write(off)
  entriesSize = strm.getPosition() - entriesSize
  
  strm.write(header)
  echo header, ' ', self.valId, ' ', self.logId, ' ', self.colId
  echo "values: ", hs(valuesSize), " columns: ", hs(columnsSize),
    " entries: ", hs(entriesSize)
  strm.close()

template bench(name: string, b: untyped) =
  echo name, ':', " (x1000)"
  var start = getMonoTime()
  for i in 1..1_000:
    b
  echo getMonoTime() - start

template smallbench(name: string, b: untyped) =
  echo name, ':', " (x1)"
  var start = getMonoTime()
  b
  echo getMonoTime() - start

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
  
  b = initBlock()
  for i in 1..<1_000_000:
    j["value"].num = i
    j["time"].num = i
    j["name"].str = rndStr()
    b.add(j)
  smallbench "b.dump":
    b.dump("out4.bin")

runBench()