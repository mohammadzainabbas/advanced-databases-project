ask_price = from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "ask_price_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> keep(columns: ["_time", "_value"])

bid_price = from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "bid_price_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> keep(columns: ["_time", "_value"])

mid = (tables=<-) =>
  tables 
      |> map(fn: (r) => ({
        _time: r._time,
        _value: (float(v: r._value_ask) + float(v: r._value_bid)) / float(v: 2)
      })
      )

join(
      tables: {ask: ask_price, bid: bid_price},
      on: ["_time"],
    )
    |> mid()
