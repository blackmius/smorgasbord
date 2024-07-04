import random
import options

type
  Node[K, V] = ref object
    key: K
    value: V
    forward: seq[Node[K, V]]

  SkipList[K, V] = ref object
    head: Node[K, V]
    level: int
    maxLevel: int
    probability: float

  SkipListIterator[K, V] = object
    current: Node[K, V]

proc newNode[K, V](key: K, value: V, level: int): Node[K, V] =
  Node[K, V](key: key, value: value, forward: newSeq[Node[K, V]](level + 1))

proc newSkipList[K, V](maxLevel: int = 32, probability: float = 0.5): SkipList[K, V] =
  let head = newNode[K, V](default(K), default(V), maxLevel)
  SkipList[K, V](head: head, level: 0, maxLevel: maxLevel, probability: probability)

proc randomLevel[K, V](sl: SkipList[K, V]): int =
  var lvl = 0
  while rand(1.0) < sl.probability and lvl < sl.maxLevel:
    inc lvl
  lvl

proc insert[K, V](sl: SkipList[K, V], key: K, value: V) =
  var update = newSeq[Node[K, V]](sl.maxLevel + 1)
  var current = sl.head

  for i in countdown(sl.level, 0):
    while current.forward[i] != nil and current.forward[i].key < key:
      current = current.forward[i]
    update[i] = current

  current = current.forward[0]

  if current != nil and current.key == key:
    current.value = value
  else:
    let newLevel = sl.randomLevel()
    if newLevel > sl.level:
      for i in sl.level + 1 .. newLevel:
        update[i] = sl.head
      sl.level = newLevel

    let newNode = newNode(key, value, newLevel)
    for i in 0 .. newLevel:
      newNode.forward[i] = update[i].forward[i]
      update[i].forward[i] = newNode

proc search[K, V](sl: SkipList[K, V], key: K): Option[V] =
  var current = sl.head
  for i in countdown(sl.level, 0):
    while current.forward[i] != nil and current.forward[i].key < key:
      current = current.forward[i]
  current = current.forward[0]
  if current != nil and current.key == key:
    some(current.value)
  else:
    none(V)

proc delete[K, V](sl: SkipList[K, V], key: K) =
  var update = newSeq[Node[K, V]](sl.maxLevel + 1)
  var current = sl.head

  for i in countdown(sl.level, 0):
    while current.forward[i] != nil and current.forward[i].key < key:
      current = current.forward[i]
    update[i] = current

  current = current.forward[0]

  if current != nil and current.key == key:
    for i in 0 .. sl.level:
      if update[i].forward[i] != current:
        break
      update[i].forward[i] = current.forward[i]

    while sl.level > 0 and sl.head.forward[sl.level] == nil:
      dec sl.level

iterator items[K, V](sl: SkipList[K, V]): (K, V) =
  var current = sl.head.forward[0]
  while current != nil:
    yield (current.key, current.value)
    current = current.forward[0]

import utils

# Пример использования
var sl = newSkipList[int, int](8, 0.125)

# sl.insert(3, "three")
# sl.insert(6, "six")
# sl.insert(7, "seven")
# sl.insert(9, "nine")
# sl.insert(12, "twelve")

# echo sl.search(7)  # Выводит: Some("seven")
# echo sl.search(10)  # Выводит: None[string]

# sl.delete(7)
# echo sl.search(7)  # Выводит: None[string]

var i = 0
bench "sl.insert", 1_000_000:
  i += 1
  sl.insert(1_000_000-i, i)

import std/tables
var table = initTable[int, int]()
bench "Table.put":
  i += 1
  table[i] = i

type N[K, V] = object
  k: K
  v: V
var s = newSeq[N[int, int]]()
bench "Seq.add":
  i += 1
  s.add(N[int, int](k: i, v: i))

# for a in sl:
#   echo a