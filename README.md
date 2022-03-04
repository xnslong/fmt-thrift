# README

This tool can format the binary thrift message into JSON format, so that we can analyze them conveniently.

```
$ cat test.bin| fmt-thrift | jq .         
{
  "1 name": "HttpHandler",
  "2 seqid": 1,
  "3 type": "reply",
  "4 body": {
    "1 STRUCT": {},
    "2 BOOL": true,
    "32 STRUCT": {}
  }
}
```