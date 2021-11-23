from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "trades")
  |> filter(fn: (r) => r["_field"] == "price")
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT" or r["symbol"] == "ETH-USDT")
  |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> pivot(rowKey:["_time"], columnKey: ["symbol"], valueColumn: "_value")
  |> map(fn: (r) => ({
    _time: r._time,
    _value: float(v: r["BTC-USDT"]) / float(v: r["ETH-USDT"])
  })
  )
  |> group()
