
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
    ## Representation of the http status code returned by the InfluxDB server when a request is made.
    OK,
      ## Maps to 2XX
    BadRequest,
      ## Maps to 400
    Unauthorized,
      ## Maps to 401
    ServerError,
      ## Maps to 5XX
    UnknownError
      ## Maps to anything else
  
  ConnectionProtocol* = enum
    ## Protocol type to be used to connect to the InfluxDB server.
    HTTP = "http",
    HTTPS = "https"
  
  InfluxDB* = object
    ## Representation of the InfluxDB host to connect to; this includes: protocol, hostname, port number, and any authentication credentials needed. Setting ``debugMode`` to `true` will cause each request and response to be printed to stdout. This functionality is strictly to aid in debugging requests made to the server.
    protocol*: ConnectionProtocol
    host*: string
    port*: int
    username*: string
    password*: string
    debugMode*: bool

  LineProtocol* = object
    ## Represenation of InfluxDB's Line Protocol used to write new data into the database. The ``tags`` and ``fields`` properties on this object are key value pairs as described by the Line Protocol documentation: https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/
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
  if influx.debugMode:
    echo("Address: " & influx_address)
    echo(" Method: " & $httpMethod)
    echo("   Body: " & body) 
  let response = client.request(influx_address, httpMethod, body)
  if influx.debugMode:
    echo("Version: " & response.version)
    echo(" Status: " & response.status)
    echo("Headers: " & $response.headers.table)
    echo("   Body: " & response.body)
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
  ## Fetches and returns the version number of the database.
  ##
  ## ``Parameters:``
  ##
  ## ``influx`` The InfluxDB Server to perform the query against
  ##
  ## ``requestType`` The type of request that is going to be made to the InfluxDB server. This defaults to "GET", and in most cases will not need to be modified
  ##
  ## ``Return Value:``
  ##
  ## ``InfluxStatus`` Enum that indicates the http status code of the response from the server
  ##
  ## ``JsonNode`` a JSON dictionary containing a single key, ``version``, which has the value that was returned in the ``x-influxdb-version`` header field of the response from the server.
  ##
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
  ## Peforms a write command to the InfluxDB server. Please note that this will be made with entirely one request. If you experience timeouts or problems writing due to bulk, consider making multiple write requests. Please follow best practices and guidelines that are outlined in the InfluxDB documentation: https://docs.influxdata.com/influxdb/v1.2/tools/api/#write
  ##
  ## ``Parameters:``
  ##
  ## ``influxdb`` The InfluxDB Server to perform the query against
  ##
  ## ``database`` The name of the database to use when making the query
  ##
  ## ``lineProtocolData`` An array of ``LineProtocol`` objects that represent the data to be written into the database
  ##
  ## ``Return Value:``
  ##
  ## ``InfluxStatus`` Enum that indicates the http status code of the response from the server
  ##
  ## ``JsonNode`` An empty JSON dictionary.
  ##
  let query_info = initTable[string, string]()
  var body_text = newSeq[string]()
  for item in lineProtocolData:
    let serialized_data = item.serializeLine()
    body_text.add(serialized_data)
  let response = influx.performSyncRequest(HttpPost, InfluxWriteEndpoint, database, query_info, body_text.join("\n"))
  return (convertHttpCodeToInfluxStatus(response.code), parseJson("{}"))

proc query*(influx: InfluxDB, database: string, query: string, httpMethod: HttpMethod = HttpGet): (InfluxStatus, JsonNode) =
  ## Performs a query against the InfluxDB server, the query string may require additional escapes to be correctly formatted when recieved by the server.
  ##
  ## ``Parameters:``
  ##
  ## ``influx`` The InfluxDB Server to perform the query against
  ##
  ## ``database`` The name of the database to use when making the query
  ##
  ## ``query`` The query string itself
  ##
  ## ``httpMethod`` The type of http request to make. Note: this defaults to using *GET*, but some queries require the use of a *POST*, please consult the official InfluxDB documentation for more information.
  ##
  ## ``Return Value:``
  ##
  ## ``InfluxStatus`` Enum that indicates the http status code of the response from the server
  ##
  ## ``JsonNode`` The body of the response.
  ##
  let queryInfo = @{"q": query}.toTable
  let response = influx.performSyncRequest(httpMethod, InfluxQueryEndpoint, database, queryInfo, nil)
  return (convertHttpCodeToInfluxStatus(response.code), parseJson(response.body))

