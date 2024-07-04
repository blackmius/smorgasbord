import options

import options

type
  Color = enum
    Red, Black

  Node[K, V] = ref object
    key: K
    value: V
    color: Color
    left, right, parent: Node[K, V]

  RedBlackTree[K, V] = object
    root: Node[K, V]
    nil_node: Node[K, V]

proc newRedBlackTree[K, V](): RedBlackTree[K, V] =
  result.nil_node = Node[K, V](color: Black)
  result.root = result.nil_node

proc leftRotate[K, V](tree: var RedBlackTree[K, V], x: Node[K, V]) =
  let y = x.right
  x.right = y.left
  if y.left != tree.nil_node:
    y.left.parent = x
  y.parent = x.parent
  if x.parent == tree.nil_node:
    tree.root = y
  elif x == x.parent.left:
    x.parent.left = y
  else:
    x.parent.right = y
  y.left = x
  x.parent = y

proc rightRotate[K, V](tree: var RedBlackTree[K, V], y: Node[K, V]) =
  let x = y.left
  y.left = x.right
  if x.right != tree.nil_node:
    x.right.parent = y
  x.parent = y.parent
  if y.parent == tree.nil_node:
    tree.root = x
  elif y == y.parent.right:
    y.parent.right = x
  else:
    y.parent.left = x
  x.right = y
  y.parent = x

proc insertFixup[K, V](tree: var RedBlackTree[K, V], z: Node[K, V]) =
  var z = z
  while z.parent.color == Red:
    if z.parent == z.parent.parent.left:
      let y = z.parent.parent.right
      if y.color == Red:
        z.parent.color = Black
        y.color = Black
        z.parent.parent.color = Red
        z = z.parent.parent
      else:
        if z == z.parent.right:
          z = z.parent
          tree.leftRotate(z)
        z.parent.color = Black
        z.parent.parent.color = Red
        tree.rightRotate(z.parent.parent)
    else:
      let y = z.parent.parent.left
      if y.color == Red:
        z.parent.color = Black
        y.color = Black
        z.parent.parent.color = Red
        z = z.parent.parent
      else:
        if z == z.parent.left:
          z = z.parent
          tree.rightRotate(z)
        z.parent.color = Black
        z.parent.parent.color = Red
        tree.leftRotate(z.parent.parent)
  tree.root.color = Black

proc insert*[K, V](tree: var RedBlackTree[K, V], key: K, value: V) =
  var y = tree.nil_node
  var x = tree.root
  while x != tree.nil_node:
    y = x
    if key < x.key:
      x = x.left
    else:
      x = x.right
  let z = Node[K, V](key: key, value: value, color: Red, left: tree.nil_node, right: tree.nil_node, parent: y)
  if y == tree.nil_node:
    tree.root = z
  elif key < y.key:
    y.left = z
  else:
    y.right = z
  tree.insertFixup(z)

proc search*[K, V](tree: RedBlackTree[K, V], key: K): Option[V] =
  var x = tree.root
  while x != tree.nil_node:
    if key == x.key:
      return some(x.value)
    elif key < x.key:
      x = x.left
    else:
      x = x.right
  none(V)

proc transplant[K, V](tree: var RedBlackTree[K, V], u, v: Node[K, V]) =
  if u.parent == tree.nil_node:
    tree.root = v
  elif u == u.parent.left:
    u.parent.left = v
  else:
    u.parent.right = v
  v.parent = u.parent

proc minimum[K, V](tree: RedBlackTree[K, V], node: Node[K, V]): Node[K, V] =
  var current = node
  while current.left != tree.nil_node:
    current = current.left
  current

proc deleteFixup[K, V](tree: var RedBlackTree[K, V], x: Node[K, V]) =
  var x = x
  while x != tree.root and x.color == Black:
    if x == x.parent.left:
      var w = x.parent.right
      if w.color == Red:
        w.color = Black
        x.parent.color = Red
        tree.leftRotate(x.parent)
        w = x.parent.right
      if w.left.color == Black and w.right.color == Black:
        w.color = Red
        x = x.parent
      else:
        if w.right.color == Black:
          w.left.color = Black
          w.color = Red
          tree.rightRotate(w)
          w = x.parent.right
        w.color = x.parent.color
        x.parent.color = Black
        w.right.color = Black
        tree.leftRotate(x.parent)
        x = tree.root
    else:
      var w = x.parent.left
      if w.color == Red:
        w.color = Black
        x.parent.color = Red
        tree.rightRotate(x.parent)
        w = x.parent.left
      if w.right.color == Black and w.left.color == Black:
        w.color = Red
        x = x.parent
      else:
        if w.left.color == Black:
          w.right.color = Black
          w.color = Red
          tree.leftRotate(w)
          w = x.parent.left
        w.color = x.parent.color
        x.parent.color = Black
        w.left.color = Black
        tree.rightRotate(x.parent)
        x = tree.root
  x.color = Black

proc delete*[K, V](tree: var RedBlackTree[K, V], key: K): bool =
  var z = tree.root
  while z != tree.nil_node:
    if key == z.key:
      var y = z
      var y_original_color = y.color
      var x: Node[K, V]
      if z.left == tree.nil_node:
        x = z.right
        tree.transplant(z, z.right)
      elif z.right == tree.nil_node:
        x = z.left
        tree.transplant(z, z.left)
      else:
        y = tree.minimum(z.right)
        y_original_color = y.color
        x = y.right
        if y.parent == z:
          x.parent = y
        else:
          tree.transplant(y, y.right)
          y.right = z.right
          y.right.parent = y
        tree.transplant(z, y)
        y.left = z.left
        y.left.parent = y
        y.color = z.color
      if y_original_color == Black:
        tree.deleteFixup(x)
      return true
    elif key < z.key:
      z = z.left
    else:
      z = z.right
  false

proc inorderTraversal[K, V](tree: RedBlackTree[K, V], node: Node[K, V], result: var seq[(K, V)]) =
  if node != tree.nil_node:
    inorderTraversal(tree, node.left, result)
    result.add((node.key, node.value))
    inorderTraversal(tree, node.right, result)

proc inorder*[K, V](tree: RedBlackTree[K, V]): seq[(K, V)] =
  result = @[]
  inorderTraversal(tree, tree.root, result)

# Пример использования
var tree = newRedBlackTree[int, int]()
import utils

var i = 0
bench "tree.insert":
  i += 1
  tree.insert(1_000_000-i, i)
# tree.insert(10, "Ten")
# tree.insert(5, "Five")
# tree.insert(15, "Fifteen")

# echo tree.search(10)  # Выводит: Some("Ten")
# echo tree.search(7)   # Выводит: None[string]