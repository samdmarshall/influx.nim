import json
import "../src/influx.nim"

let influxdb = initializeDefaultInflux()
let (version_response, version_data) = influxdb.getInfluxInfo()
echo(version_response)
echo($version_data)

let (write_response, write_data) = influxdb.write("fish_history", @["cmd_hist command=\"echo\",arguments=\"hello\",location=\"/Users/Samantha/Projects/influx.nim\""])
echo(write_response)
echo($write_data)

let (select_response, select_data) = influxdb.query("fish_history", "SELECT * FROM cmd_hist")
echo(select_response)
echo($select_data)
