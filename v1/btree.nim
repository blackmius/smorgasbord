const
  M = 14    # max children per B-tree node = M-1
             # (must be even and greater than 2)
  Mhalf = M div 2

type
  Node[Key, Val] {.acyclic.} = ref object
    entries: int
    keys: array[M, Key]
    case isInternal: bool
    of false:
      vals: array[M, Val]
    of true:
      links: array[M, Node[Key, Val]]
  BTree*[Key, Val] = object
    root: Node[Key, Val]
    entries: int      ## number of key-value pairs
    cmpFn: proc (a, b: Key): int

proc defaultCmp[Key](a, b: Key): int = cmp(a, b)

proc initBTree*[Key, Val](
  cmpFn: proc (a, b: Key): int = defaultCmp
): BTree[Key, Val] =
  BTree[Key, Val](root: Node[Key, Val](entries: 0, isInternal: false),
    cmpFn: cmpFn)

template less[Key, Val](t: BTree[Key, Val], a, b: Key): bool = t.cmpFn(a, b) < 0
template eq[Key, Val](t: BTree[Key, Val], a, b: Key): bool = t.cmpFn(a, b) == 0

proc getOrDefault*[Key, Val](b: BTree[Key, Val], key: Key): Val =
  var x = b.root
  while x.isInternal:
    for j in 0..<x.entries:
      if j+1 == x.entries or b.less(key, x.keys[j+1]):
        x = x.links[j]
        break
  assert(not x.isInternal)
  for j in 0..<x.entries:
    if b.eq(key, x.keys[j]): return x.vals[j]

proc contains*[Key, Val](b: BTree[Key, Val], key: Key): bool =
  var x = b.root
  while x.isInternal:
    for j in 0..<x.entries:
      if j+1 == x.entries or b.less(key, x.keys[j+1]):
        x = x.links[j]
        break
  assert(not x.isInternal)
  for j in 0..<x.entries:
    if b.eq(key, x.keys[j]): return true
  return false

proc copyHalf[Key, Val](h, result: Node[Key, Val]) =
  for j in 0..<Mhalf:
    result.keys[j] = h.keys[Mhalf + j]
  if h.isInternal:
    for j in 0..<Mhalf:
      result.links[j] = h.links[Mhalf + j]
  else:
    for j in 0..<Mhalf:
      when defined(gcArc) or defined(gcOrc) or defined(gcAtomicArc):
        result.vals[j] = move h.vals[Mhalf + j]
      else:
        shallowCopy(result.vals[j], h.vals[Mhalf + j])

proc split[Key, Val](h: Node[Key, Val]): Node[Key, Val] =
  ## split node in half
  result = Node[Key, Val](entries: Mhalf, isInternal: h.isInternal)
  h.entries = Mhalf
  copyHalf(h, result)

proc insert[Key, Val](b: BTree[Key, Val], h: Node[Key, Val], key: Key, val: Val): Node[Key, Val] =
  #var t = Entry(key: key, val: val, next: nil)
  var newKey = key
  var j = 0
  if not h.isInternal:
    while j < h.entries:
      # to replace
      # if b.eq(key, h.keys[j]):
      #   h.vals[j] = val
      #   return
      if b.less(key, h.keys[j]): break
      inc j
    for i in countdown(h.entries, j+1):
      when defined(gcArc) or defined(gcOrc) or defined(gcAtomicArc):
        h.vals[i] = move h.vals[i-1]
      else:
        shallowCopy(h.vals[i], h.vals[i-1])
    h.vals[j] = val
  else:
    var newLink: Node[Key, Val] = nil
    while j < h.entries:
      if j+1 == h.entries or b.less(key, h.keys[j+1]):
        let u = b.insert(h.links[j], key, val)
        inc j
        if u == nil: return nil
        newKey = u.keys[0]
        newLink = u
        break
      inc j
    for i in countdown(h.entries, j+1):
      h.links[i] = h.links[i-1]
    h.links[j] = newLink

  for i in countdown(h.entries, j+1):
    h.keys[i] = h.keys[i-1]
  h.keys[j] = newKey
  inc h.entries
  return if h.entries < M: nil else: split(h)

proc add*[Key, Val](b: var BTree[Key, Val]; key: Key; val: Val) =
  let u = b.insert(b.root, key, val)
  inc b.entries
  if u == nil: return

  # need to split root
  let t = Node[Key, Val](entries: 2, isInternal: true)
  t.keys[0] = b.root.keys[0]
  t.links[0] = b.root
  t.keys[1] = u.keys[0]
  t.links[1] = u
  b.root = t

proc toString[Key, Val](h: Node[Key, Val], indent: string; result: var string) =
  if not h.isInternal:
    for j in 0..<h.entries:
      result.add(indent)
      result.add($h.keys[j] & " " & $h.vals[j] & "\n")
  else:
    for j in 0..<h.entries:
      if j > 0: result.add(indent & "(" & $h.keys[j] & ")\n")
      toString(h.links[j], indent & "   ", result)

proc `$`[Key, Val](b: BTree[Key, Val]): string =
  result = ""
  toString(b.root, "", result)

proc hasNext*[Key, Val](b: BTree[Key, Val]; index: int): bool = index < b.entries

proc countSubTree[Key, Val](it: Node[Key, Val]): int =
  if it.isInternal:
    result = 0
    for k in 0..<it.entries:
      inc result, countSubTree(it.links[k])
  else:
    result = it.entries

proc next*[Key, Val](b: BTree[Key, Val]; index: int): (Key, Val, int) =
  var it = b.root
  var i = index
  # navigate to the right leaf:
  while it.isInternal:
    var sum = 0
    for k in 0..<it.entries:
      let c = countSubTree(it.links[k])
      inc sum, c
      if sum > i:
        it = it.links[k]
        dec i, (sum - c)
        break
  result = (it.keys[i], it.vals[i], index+1)

iterator pairs*[Key, Val](b: BTree[Key, Val]): (Key, Val) =
  var i = 0
  while hasNext(b, i):
    let (k, v, i2) = next(b, i)
    i = i2
    yield (k, v)

proc len*[Key, Val](b: BTree[Key, Val]): int {.inline.} = b.entries

when isMainModule:
  import utils
  var t = initBTree[int, int]()
  var i = 0
  bench "BTree.add":
    i += 1
    t.add(1_000_000-i, i)
  import std/tables
  var table = initTable[int, int]()
  bench "Table.put":
    i += 1
    table[i] = i
  # echo t