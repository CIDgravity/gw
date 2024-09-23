## Basic file info
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


## detailed file info

```
success: <bool>
error: <message>                                # only if success is false
result:                                         # only if success is true
  file:                                        
    cid: <cid>	                                # File CID to retrieve directly on IPFS
    details:                                    # Only if verbose: true is passed
      state: <fileState>
      retrievableCopies: <int>			# mininum retrievableCopies of the different groups
      groups:
      - state: <groupState>
        id: <pieceCID>                          # block PieceCID if available
        retrievableCopies: <int>
	deals:					# only if in a suffisent state to have some
	- provider: <string>
          state: <string>
          dealId: <int>
	  endEpoch: <int>
          isRetrievable: <bool>
```

#### States

##### DealState

```
# Pretty self describing
        DealStateProposed = "proposed"
        DealStatePublished = "published"
        DealStateActive = "active"
```

##### Group State

```
# Group status from ribs internal naming
        GroupStateWritable = "writable"
        GroupStateFull = "full"
        GroupStateVRCARDone = "VRCARDone"
        GroupStateReadyForDeals = "ready for deals"
        GroupStateOffloaded = "offloaded"
        GroupStateReload = "reload"
```

##### File State

```
# if at least 1 group not yet ready (writable/full/VRCAR done)
        FileStateStaging = "staging"		
# if all groups are ready
        FileStateOffloading = "offloading"	
# if all groups have at least 1 active deal
        FileStatePartiallyOffload = "partially offloaded" 
# If all groups are offloaded
        FileStateOffloaded = "offloaded"
```
