
# =======
# Imports
# =======

import cgi
import uri
import json
import tables
import strutils
import httpclient

# =========
# Constants
# =========

const
  InfluxPingEndpoint = "/ping"
  InfluxQueryEndpoint = "/query"
  InfluxWriteEndpoint = "/write"

  InfluxVersionHeader = "x-influxdb-version"

# =====
# Types
# =====

type
  InfluxStatus* = enum
    OK,
    BadRequest,
    Unauthorized,
    ServerError,
    UnknownError
  
  ConnectionProtocol* = enum
    HTTP = "http",
    HTTPS = "https"
  
  InfluxDB* = object
    protocol*: ConnectionProtocol
    host*: string
    port*: int
    username*: string
    password*: string

  LineProtocol* = object
    measurement*: string
    tags*: Table[string, string]
    fields*: Table[string, string]
    timestamp*: int64

# =================
# Private Functions
# =================

proc composeInfluxHostname(influx: InfluxDB, endpoint: string, database: string, additionalInfo: Table[string, string]): string =
  var complete_uri = initUri()

  complete_uri.scheme = $influx.protocol
  complete_uri.hostname = influx.host
  complete_uri.port = $influx.port
  complete_uri.path = endpoint

  if database.len > 0:
    var database_query = initUri()
    database_query.query = "db=" & database
    complete_uri = combine(complete_uri, database_query)

  if (influx.username.len > 0) and (influx.password.len > 0):
    complete_uri.username = influx.username
    complete_uri.password = influx.password

  for key, value in additionalInfo:
    var query_uri = initUri()
    query_uri.query = complete_uri.query & "&" & key & "=" & encodeUrl(value)
    complete_uri = combine(complete_uri, query_uri)
    
  return $complete_uri

proc performSyncRequest(influx: InfluxDB, httpMethod: HttpMethod, endpoint: string, database: string, additionalInfo: Table[string, string], body: string): Response =
  let client = newHttpClient()
  let influx_address = composeInfluxHostname(influx, endpoint, database, additionalInfo)
  let response = client.request(influx_address, httpMethod, body)
  return response

proc convertHttpCodeToInfluxStatus(code: HttpCode): InfluxStatus =
  if is2xx(code):
    return OK
  if is4xx(code):
    case code
    of HttpCode(400):
      return BadRequest
    of HttpCode(401):
      return Unauthorized
    else:
      discard
  if is5xx(code):
    return ServerError
  return UnknownError

proc serializeLine(line_data: LineProtocol): string =
  var output = line_data.measurement
  for key, value in line_data.tags:
    output &= ("," & key & "=" & value)
  output &= " "
  var fields = newSeq[string]()
  for key, value in line_data.fields:
    fields.add( key & "=\"" & value & "\"" )
  output &= fields.join(",")
  output &= " "
  if line_data.timestamp != 0:
    output &= $line_data.timestamp
  return output

# ================
# Public Functions
# ================

proc getVersion*(influx: InfluxDB, requestType: HttpMethod = HttpGet): (InfluxStatus, JsonNode) =
  let query_info = initTable[string, string]()
  case requestType
  of HttpGet, HttpHead:
    let response = influx.performSyncRequest(requestType, InfluxPingEndpoint, nil, query_info, nil)
    if response.headers.table.hasKey(InfluxVersionHeader):
      let version_number = response.headers.table[InfluxVersionHeader].join(" ")
      let version_json = "{\"version\": \"" & version_number & "\"}"
      return (convertHttpCodeToInfluxStatus(response.code), parseJson(version_json))
  else:
    raise newException(HttpRequestError, "POST requests are not allowed when performing a ping")

proc write*(influx: InfluxDB, database: string, lineProtocolData: seq[LineProtocol]): (InfluxStatus, JsonNode) = 
  let query_info = initTable[string, string]()
  var body_text = newSeq[string]()
  for item in lineProtocolData:
    let serialized_data = item.serializeLine()
    body_text.add(serialized_data)
  let response = influx.performSyncRequest(HttpPost, InfluxWriteEndpoint, database, query_info, body_text.join("\n"))
  return (convertHttpCodeToInfluxStatus(response.code), parseJson("{}"))

proc query*(influx: InfluxDB, database: string, query: string, httpMethod: HttpMethod = HttpGet): (InfluxStatus, JsonNode) =
  let queryInfo = @{"q": query}.toTable
  let response = influx.performSyncRequest(httpMethod, InfluxQueryEndpoint, database, queryInfo, nil)
  return (convertHttpCodeToInfluxStatus(response.code), parseJson(response.body))

