import std/json
import std/tables
import pkg/cbor

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
