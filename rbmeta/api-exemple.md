```
$ curl -s -X POST http://localhost:9011/file-info -d '{"filePath": "/"}' | jq .
{
  "success": true,
  "result": {
    "file": {
      "cid": "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn"
    }
  }
}
$ curl -s -X POST http://localhost:9011/file-info -d '{"filePath": "/non-existent"}' | jq .
{
  "success": false,
  "error": "File not found"
}
```
