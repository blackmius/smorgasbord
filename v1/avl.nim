import options

type
  Node[K, V] = ref object
    key: K
    value: V
    left, right: Node[K, V]
    height: int

proc height[K, V](n: Node[K, V]): int =
  if n == nil: 0 else: n.height

proc updateHeight[K, V](n: Node[K, V]) =
  n.height = max(height(n.left), height(n.right)) + 1

proc balanceFactor[K, V](n: Node[K, V]): int =
  height(n.left) - height(n.right)

proc rotateRight[K, V](y: Node[K, V]): Node[K, V] =
  let x = y.left
  let T2 = x.right

  x.right = y
  y.left = T2

  updateHeight(y)
  updateHeight(x)
  x

proc rotateLeft[K, V](x: Node[K, V]): Node[K, V] =
  let y = x.right
  let T2 = y.left

  y.left = x
  x.right = T2

  updateHeight(x)
  updateHeight(y)
  y

proc insert[K, V](root: Node[K, V], key: K, value: V): Node[K, V] =
  if root == nil:
    return Node[K, V](key: key, value: value, height: 1)

  if key < root.key:
    root.left = insert(root.left, key, value)
  elif key > root.key:
    root.right = insert(root.right, key, value)
  else:
    root.value = value  # Обновляем значение, если ключ уже существует
    return root

  updateHeight(root)

  let balance = balanceFactor(root)

  # Left Left Case
  if balance > 1 and key < root.left.key:
    return rotateRight(root)

  # Right Right Case
  if balance < -1 and key > root.right.key:
    return rotateLeft(root)

  # Left Right Case
  if balance > 1 and key > root.left.key:
    root.left = rotateLeft(root.left)
    return rotateRight(root)

  # Right Left Case
  if balance < -1 and key < root.right.key:
    root.right = rotateRight(root.right)
    return rotateLeft(root)

  root

proc get[K, V](root: Node[K, V], key: K): Option[V] =
  if root == nil:
    return none(V)
  
  if key < root.key:
    return get(root.left, key)
  elif key > root.key:
    return get(root.right, key)
  else:
    return some(root.value)

proc inOrder[K, V](root: Node[K, V]) =
  if root != nil:
    inOrder(root.left)
    echo root.key, ": ", root.value
    inOrder(root.right)

# Пример использования
var root: Node[int, int] = nil
import utils

var i = 0
bench "tree.insert":
  i += 1
  root = root.insert(1_000_000-i, i)

# inOrder(root)

# root = insert(root, 10, "Ten")
# root = insert(root, 20, "Twenty")
# root = insert(root, 30, "Thirty")
# root = insert(root, 40, "Forty")
# root = insert(root, 50, "Fifty")
# root = insert(root, 25, "Twenty-Five")

# echo "Inorder traversal of the constructed AVL tree is:"
# inOrder(root)

# echo "\nGetting values:"
# echo "Key 20: ", get(root, 20)
# echo "Key 35: ", get(root, 35)