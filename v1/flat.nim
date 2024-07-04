import std/json
import std/tables

type
  FlatField* = object
    name*: string
    val*: JsonNode
  FlatEntry* = seq[FlatField]

proc flatEntry*(arr: var FlatEntry, e: JsonNode, path: string) =
  for k, v in e.fields.pairs:
    if unlikely v.kind == JObject:
      flatEntry(arr, v, path & k & ".")
    else:
      arr.add FlatField(name: k, val: v)

proc flatEntry*(e: JsonNode): FlatEntry =
  result = newSeqOfCap[FlatField](e.len)
  flatEntry(result, e, "")
