import std/math
import std/hashes

type BloomFilter* = object
  data: seq[int64]
  len: int
  n: int
  p: float64
  k: int

proc initBloomFilter*(n: int, p: float): BloomFilter =
  const ln2 = ln(2.float32)
  const ln2sqr = ln2*ln2
  let bits = int(-(n.float64*math.ln(p))/ln2sqr)
  result.data = newSeq[int64](bits div 64)
  result.k = int((bits/n)*ln2)
  result.n = n
  result.p = p

proc incl*(bf: var BloomFilter, v: int) =
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

proc contains*(bf: var BloomFilter, v: int): bool =
  var q = v.uint64
  for i in 1..bf.k:
    q = hash(q).uint64
    let bit = q mod (bf.data.len*64).uint64
    let index = bit div 64
    if (bf.data[index] and (1 shl (bit mod 64))) == 0:
      return false
  return true
