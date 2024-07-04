type Count*[T] = object
  val*: T
  count*: int

proc RLE*[T](arr: openArray[T]): seq[Count[T]] =
  # XXX: checkout https://github.com/powturbo/Turbo-Run-Length-Encoding
  result = newSeq[Count[T]]()
  var count = 1
  for i in 0..<arr.high:
    if arr[i] != arr[i+1]:
      result.add Count[T](val: arr[i], count: count)
      count = 0
    count += 1
  if arr.len > 0:
    result.add Count[T](val: arr[^1], count: count)

proc deltaEncode*[T](arr: openArray[T]): seq[T] =
  # XXX: SIMD (its second vector is just same with offset 1)
  result = newSeq[T](arr.len)
  if arr.len == 0:
    return
  result[0] = arr[0]
  for i in 1..arr.high:
    result[i] = arr[i]-arr[i-1]
