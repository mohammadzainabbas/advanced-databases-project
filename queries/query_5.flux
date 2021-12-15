import "math"

from(bucket: "advanced_db")
  |> range(start: _start, stop: _end)
  |> filter(fn: (r) => r["_measurement"] == "trades")
  |> filter(fn: (r) => r["_field"] == "price")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
  |> window(every: 5m)
  |> reduce(
    identity: { open: float(v: 0), high: -1.0, low: float(v: math.maxfloat), close: float(v: 0), is_first: true},
    fn: (r, accumulator) => ({
      open: if accumulator.is_first then r._value else accumulator.open,
      high: math.mMax(x: r._value, y: accumulator.high),
      low: math.mMin(x: r._value, y: accumulator.low),
      close: r._value,
      is_first: false
      })
    )
  |> rename(columns: {_stop: "_time" })
  |> keep(columns: ["_time", "symbol", "open", "high", "low", "close"])
  |> group()
