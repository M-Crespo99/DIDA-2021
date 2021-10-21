#/bin/sh

#create a storage
~/Downloads/grpcurl --plaintext -d '{"id": 1, "url": "localhost:10000", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createStorage
sleep 1
#crate a worker
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:10000", "gossip_delay": 200}' localhost:2000 PuppetMasterService/createWorker
sleep 1
#create a scheduler
~/Downloads/grpcurl --plaintext -d '{"url": "localhost:10000"}' localhost:2000 PuppetMasterService/createScheduler
sleep 1

#run the script
~/Downloads/grpcurl --plaintext -d '{"input": "123456", "filePath": "/home/miguelcrespo/Documents/Tecnico/4Ano/DAD/DIDA-2021/scheduler/scripts/operator_scripts/app1", "workers": [ "localhost:5002"], "storages": [ "localhost:5001" ], "schedulerUrl": "localhost:5003"}' localhost:2000 PuppetMasterService/runApplication