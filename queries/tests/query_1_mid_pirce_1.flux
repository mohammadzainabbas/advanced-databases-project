# Not complete

data = from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "ask_price_1" or r["_field"] == "bid_price_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)

get_data = (x) =>
  data
    |> filter(fn: (r) => r._field == x)
    |> yield(name: x)

get_data(x: "ask_price_1")
get_data(x: "bid_price_1")

=============

# Correct but join all tables

from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "ask_price_1" or r["_field"] == "bid_price_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> mean()
  |> keep(columns: ["_time", "_value"])

or 

from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "ask_price_1" or r["_field"] == "bid_price_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> mean()
  |> keep(columns: ["_time", "_value"])
