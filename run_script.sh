

~/Downloads/grpcurl --plaintext -d '{"input": "123456", "filePath": "app2", "workers": [ "localhost:5002", "localhost:5003"], "storages": [ "localhost:5001" ], "schedulerUrl": "localhost:5004"}' localhost:2000 PuppetMasterService/runApplication