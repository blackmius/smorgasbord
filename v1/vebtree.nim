import math

type
  VEBTree[T] = ref object
    u: int  # Размер вселенной
    min: int  # Минимальный ключ
    max: int  # Максимальный ключ
    minValue: T  # Значение, соответствующее минимальному ключу
    maxValue: T  # Значение, соответствующее максимальному ключу
    summary: VEBTree[T]  # Вспомогательное дерево
    cluster: seq[VEBTree[T]]  # Кластеры

proc newVEBTree[T](u: int): VEBTree[T] =
  if u <= 2:
    return VEBTree[T](u: u, min: -1, max: -1)
  
  let upperSqrt = 1 shl ((int(math.log2(u.float32)) + 1) div 2)
  let lowerSqrt = 1 shl (int(math.log2(u.float32)) div 2)
  
  result = VEBTree[T](
    u: u,
    min: -1,
    max: -1,
    summary: newVEBTree[T](upperSqrt),
    cluster: newSeq[VEBTree[T]](upperSqrt)
  )
  for i in 0..<upperSqrt:
    result.cluster[i] = newVEBTree[T](lowerSqrt)

proc high[T](tree: VEBTree[T], x: int): int =
  1 shl ((int(math.log2(x.float32)) + 1) div 2)

proc low[T](tree: VEBTree[T], x: int): int =
  x mod tree.high(tree.u)

proc index[T](tree: VEBTree[T], x, y: int): int =
  x * tree.high(tree.u) + y

proc insert[T](tree: var VEBTree[T], key: int, value: T) =
  if tree.min == -1:
    tree.min = key
    tree.max = key
    tree.minValue = value
    tree.maxValue = value
    return

  if key < tree.min:
    tree.min = key
    tree.minValue = value

  if tree.u > 2:
    let h = tree.high(tree.u)
    let i = key div h
    if tree.cluster[i].min == -1:
      tree.summary.insert(i, value)
      tree.cluster[i].min = key mod h
      tree.cluster[i].max = key mod h
      tree.cluster[i].minValue = value
      tree.cluster[i].maxValue = value
    else:
      tree.cluster[i].insert(key mod h, value)

  if key > tree.max:
    tree.max = key
    tree.maxValue = value

proc get[T](tree: VEBTree[T], key: int): T =
  if key == tree.min:
    return tree.minValue
  elif key == tree.max:
    return tree.maxValue
  elif tree.u == 2:
    raise newException(KeyError, "Key not found")
  else:
    let h = tree.high(tree.u)
    let i = key div h
    return tree.cluster[i].get(key mod h)

proc contains[T](tree: VEBTree[T], key: int): bool =
  if key == tree.min or key == tree.max:
    return true
  elif tree.u == 2:
    return false
  else:
    let h = tree.high(tree.u)
    let i = key div h
    return tree.cluster[i].contains(key mod h)

proc successor[T](tree: VEBTree[T], key: int): (int, T) =
  if tree.u == 2:
    if key == 0 and tree.max == 1:
      return (1, tree.maxValue)
    else:
      return (-1, default(T))
  elif tree.min != -1 and key < tree.min:
    return (tree.min, tree.minValue)
  else:
    let h = tree.high(tree.u)
    let i = key div h
    let j = key mod h
    
    var maxLow: int
    if tree.cluster[i] != nil:
      maxLow = tree.cluster[i].max
    else:
      maxLow = -1

    if maxLow != -1 and j < maxLow:
      let (offset, value) = tree.cluster[i].successor(j)
      return (tree.index(i, offset), value)
    else:
      let (succCluster, _) = tree.summary.successor(i)
      if succCluster == -1:
        return (-1, default(T))
      else:
        let (offset, value) = (tree.cluster[succCluster].min, tree.cluster[succCluster].minValue)
        return (tree.index(succCluster, offset), value)

# Пример использования
var tree = newVEBTree[int](1_000_000)
tree.insert(2, 2)
tree.insert(3, 3)
tree.insert(4, 4)
tree.insert(5, 5)
tree.insert(7, 7)
tree.insert(14, 14)
tree.insert(15, 15)

echo tree.contains(3)  # true
echo tree.contains(6)  # false
echo tree.get(3)  # "Three"
echo tree.successor(5)  # (7, "Seven")
echo tree.successor(6)  # (7, "Seven")
let (key, value) = tree.successor(15)
echo key, " ", value  # -1 "" (нет преемника)

import std/sequtils
import std/[times, monotimes]
import std/math

template bench(name: string, b: untyped) =
  echo name, ':', " (x1000)"
  var start = getMonoTime()
  for i in 1..1_000:
    b
  let dur = getMonoTime() - start
  echo dur
  echo math.floor(1_000_000_000 / (dur.inNanoseconds/1000)), ' ', "rps"

var i = 0
bench "tree.insert":
  i += 1
  tree.insert(i+23, i)

# echo tree.successor()