import json
import tables
import "../src/influx.nim"

let influxdb = InfluxDB(protocol: HTTP, host: "localhost", port: 8086)
let (version_response, version_data) = influxdb.getVersion()
echo(version_response)
echo($version_data)

let values = @{
  "command": "echo",
  "arguments": "hello",
  "location": "/Users/Samantha/Projects/influx.nim/",
}.toTable
let data = LineProtocol(measurement: "cmd_hist", fields: values)
let (write_response, write_data) = influxdb.write("fish_history", @[data])
echo(write_response)
echo($write_data)

let (select_response, select_data) = influxdb.query("fish_history", "SELECT * FROM cmd_hist")
echo(select_response)
echo($select_data)
