from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => 
    r["_field"] == "ask_price_1" or 
    r["_field"] == "ask_volume_1" or 
    r["_field"] == "ask_price_2" or 
    r["_field"] == "ask_volume_2" or 
    r["_field"] == "ask_price_3" or 
    r["_field"] == "ask_volume_3" or 
    r["_field"] == "ask_price_4" or 
    r["_field"] == "ask_volume_4" or 
    r["_field"] == "ask_price_5" or 
    r["_field"] == "ask_volume_5" or 
    r["_field"] == "bid_price_1" or 
    r["_field"] == "bid_volume_1" or 
    r["_field"] == "bid_price_2" or 
    r["_field"] == "bid_volume_2" or 
    r["_field"] == "bid_price_3" or 
    r["_field"] == "bid_volume_3" or 
    r["_field"] == "bid_price_4" or 
    r["_field"] == "bid_volume_4" or 
    r["_field"] == "bid_price_5" or 
    r["_field"] == "bid_volume_5"
    )
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
    _time: r._time,
    mid: (float(v: r.ask_price_1) + float(v: r.bid_price_1)) / float(v: 2),
    ask_price: (float(v: r.ask_price_1) + float(v: r.ask_price_2) + float(v: r.ask_price_3) + float(v: r.ask_price_4) + float(v: r.ask_price_5)) / float(v: 5),
    ask_volume: (float(v: r.ask_volume_1) + float(v: r.ask_volume_2) + float(v: r.ask_volume_3) + float(v: r.ask_volume_4) + float(v: r.ask_volume_5)) / float(v: 5),
    bid_price: (float(v: r.bid_price_1) + float(v: r.bid_price_2) + float(v: r.bid_price_3) + float(v: r.bid_price_4) + float(v: r.bid_price_5)) / float(v: 5),
    bid_volume: (float(v: r.bid_volume_1) + float(v: r.bid_volume_2) + float(v: r.bid_volume_3) + float(v: r.bid_volume_4) + float(v: r.bid_volume_5)) / float(v: 5),
  }))
  |> map(fn: (r) => ({
    _time: r._time,
    _value: r.mid - (((float(v: r.ask_price) * float(v: r.bid_volume)) + (float(v: r.bid_price) * float(v: r.ask_volume))) / (float(v: r.ask_volume) + float(v: r.bid_volume)))
  }))
  |> keep(columns: ["_time", "_value"])
  |> group()

==========================

from(bucket: "advanced_db")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "order_book")
  |> filter(fn: (r) => 
    r["_field"] == "ask_price_1" or 
    r["_field"] == "ask_volume_1" or 
    r["_field"] == "ask_price_2" or 
    r["_field"] == "ask_volume_2" or 
    r["_field"] == "ask_price_3" or 
    r["_field"] == "ask_volume_3" or 
    r["_field"] == "ask_price_4" or 
    r["_field"] == "ask_volume_4" or 
    r["_field"] == "ask_price_5" or 
    r["_field"] == "ask_volume_5" or 
    r["_field"] == "bid_price_1" or 
    r["_field"] == "bid_volume_1" or 
    r["_field"] == "bid_price_2" or 
    r["_field"] == "bid_volume_2" or 
    r["_field"] == "bid_price_3" or 
    r["_field"] == "bid_volume_3" or 
    r["_field"] == "bid_price_4" or 
    r["_field"] == "bid_volume_4" or 
    r["_field"] == "bid_price_5" or 
    r["_field"] == "bid_volume_5"
    )
  |> filter(fn: (r) => r["symbol"] == "BTC-USDT")
  |> group(columns: ["_time", "_measurement"], mode:"by")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
    _time: r._time,
    _value: ((float(v: r.ask_price_1) + float(v: r.bid_price_1)) / float(v: 2)) - (((((float(v: r.ask_price_1) + float(v: r.ask_price_2) + float(v: r.ask_price_3) + float(v: r.ask_price_4) + float(v: r.ask_price_5)) / float(v: 5)) * ((float(v: r.ask_volume_1) + float(v: r.ask_volume_2) + float(v: r.ask_volume_3) + float(v: r.ask_volume_4) + float(v: r.ask_volume_5)) / float(v: 5))) + (((float(v: r.bid_price_1) + float(v: r.bid_price_2) + float(v: r.bid_price_3) + float(v: r.bid_price_4) + float(v: r.bid_price_5)) / float(v: 5)) * ((float(v: r.bid_volume_1) + float(v: r.bid_volume_2) + float(v: r.bid_volume_3) + float(v: r.bid_volume_4) + float(v: r.bid_volume_5)) / float(v: 5)))) / (((float(v: r.bid_volume_1) + float(v: r.bid_volume_2) + float(v: r.bid_volume_3) + float(v: r.bid_volume_4) + float(v: r.bid_volume_5)) / float(v: 5)) + ((float(v: r.ask_volume_1) + float(v: r.ask_volume_2) + float(v: r.ask_volume_3) + float(v: r.ask_volume_4) + float(v: r.ask_volume_5)) / float(v: 5))))
  }))
  |> keep(columns: ["_time", "_value"])
  |> group()
