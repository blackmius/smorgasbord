import std/tables
import std/json

import pkg/cbor

import ./btree
import ./utils
import ./flat
import ./cbor_json

type 
  Id = uint32
  Column = ref object
    id: Id
    val2log: Btree[Id, Id]
    log2val: Btree[Id, Id]

  Chunk = ref object
    logId: Id
    colId: Id
    valId: Id
    columns: Table[Id, Column]
    id2Val: seq[string]
    val2Id: Table[string, Id]

proc initChunk(): Chunk =
  new result
  result.columns = initTable[Id, Column]()
  result.val2Id = initTable[string, Id]()
  result.id2Val = newSeq[string]()

proc add(self: Chunk, entry: JsonNode) =
  let fe = flatEntry(entry)
  let logId = self.logId
  self.logId += 1

  for field in fe:
    let field_name = cbor.encode(field.name)
    if not self.val2Id.hasKey(field_name):
      self.val2Id[field_name] = self.valId
      self.id2Val.add(field_name)
      self.columns[self.valId] = Column(
        id: self.valId,
        log2Val: initBTree[Id, Id](),
        val2Log: initBTree[Id, Id](
          proc (a, b: Id): int =
            cmp(self.id2Val[a], self.id2Val[b])
        )
      )
      self.valId += 1
    let colId = self.val2Id[field_name]
    let column = self.columns[colId]
    let val = cbor.encode(field.val.toCbor)
    if not self.val2Id.hasKey(val):
      self.val2Id[val] = self.valId
      self.id2Val.add(val)
      self.valId += 1
    let valId = self.val2Id[val]
    column.log2val.add(logId, valId)
    column.val2log.add(valId, logId)

when isMainModule:
  import std/times
  var chunk = initChunk()
  let jjj = readFile("k8slog_json.json")
  let jj = parseJson(jjj)
  var i = 0
  bench "chunk.add":
    i += 1
    var j = jj[i mod jj.len]
    discard flatEntry(j)
    # j["value"].num = i
    # j["time"].num = i
    # j["name"] = %* rndStr()
    # chunk.add(j)