#/bin/sh

#create a storage
~/Downloads/grpcurl --plaintext -d '{"id": "1", "url": "localhost:5001", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createStorage
#crate a worker
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:5002", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createWorker
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:5003", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createWorker
#create a scheduler
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:5000"}' localhost:2000 PuppetMasterService/createScheduler