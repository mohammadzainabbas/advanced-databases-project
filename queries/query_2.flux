from(bucket: "advanced_db")
  |> range(start: _start, stop: _end)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => r["_field"] == "ask_price_1" or r["_field"] == "ask_volume_1" or r["_field"] == "bid_price_1" or r["_field"] == "bid_volume_1")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
    _time: r._time,
    _value: ((float(v: 0.5)) * (float(v: r.ask_price_1) + float(v: r.bid_price_1))) - (((float(v: r.ask_price_1) * float(v: r.bid_volume_1)) + (float(v: r.bid_price_1) * float(v: r.ask_volume_1))) / (float(v: r.ask_volume_1) + float(v: r.bid_volume_1)))
  })
  )
  |> group()
