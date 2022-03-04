# README

This tool can format the binary thrift message into JSON format, so that we can analyze them conveniently.


For example, typing the following command to format the input thrift message,

```
$ printf "\x80\x01\x00\x02\x00\x00\x00\x0aRpcHandler\x00\x00\x00\x01\x0c\x00\x01\x00\x02\x00\x02\x01\x0c\x00\x20\x00\x00" |\
  fmt-thrift|\
  jq .
```

it will output the following JSON.

```json
{
  "1 name": "RpcHandler",
  "2 seqid": 1,
  "3 type": "reply",
  "4 body": {
    "1 STRUCT": {},
    "2 BOOL": true,
    "32 STRUCT": {}
  }
}
```
