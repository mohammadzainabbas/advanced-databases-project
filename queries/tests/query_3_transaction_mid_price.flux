from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book" or r["_measurement"] == "trades")
  |> filter(fn: (r) => r["_field"] == "ask_price_1" or r["_field"] == "bid_price_1" or r["_field"] == "price")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> group(columns: ["_time"], mode:"by")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
    _time: r._time,
    _value: ((float(v: 0.5)) * (float(v: r.ask_price_1) + float(v: r.bid_price_1))) - float(v: r.price)
  })
  )
  |> group()
