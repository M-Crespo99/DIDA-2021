#/bin/sh

#create a storage
~/Downloads/grpcurl --plaintext -d '{"id": 1, "url": "localhost:10000", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createStorage
#crate a worker
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:10000", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createWorker
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:10000", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createWorker
#create a scheduler
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:10000"}' localhost:2000 PuppetMasterService/createScheduler