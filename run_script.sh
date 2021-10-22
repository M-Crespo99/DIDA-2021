# fullname="USER INPUT"
read -p "Enter app you want to run: " app
# user="USER INPUT"
read -p "Enter Input for the app: " input



~/Downloads/grpcurl --plaintext -d '{"input": "'${input}'", "filePath": "'${app}'", "workers": [ "localhost:5002", "localhost:5003"], "storages": [ "localhost:5001" ], "schedulerUrl": "localhost:5004"}' localhost:2000 PuppetMasterService/runApplication