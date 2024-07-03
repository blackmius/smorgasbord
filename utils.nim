import std/sequtils
import std/[times, monotimes]
import std/math

template bench*(name: string, count: int, b: untyped) =
  echo name, ':', " (x", count, ")"
  var start = getMonoTime()
  for i in 1..count:
    b
  let dur = getMonoTime() - start
  echo dur
  echo math.floor(1_000_000_000 / (dur.inNanoseconds/count)), ' ', "rps"
  echo "one OP: ", dur.inNanoseconds/count

template bench*(name: string, b: untyped) = bench(name, 1_000_000, b)
template smallbench*(name: string, b: untyped) = bench(name, 1, b)

proc hs*(bytes: int, dp=1): string =
  const thresh = 1024
  if bytes < thresh:
    return $(bytes) & " B"
  var b = float(bytes)
  const units = @["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
  var u = -1
  while b > thresh and u < units.high:
    b /= thresh
    u += 1
  return $math.round(b, dp) & ' ' & units[u]