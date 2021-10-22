# fullname="USER INPUT"
read -p "Enter app you want to run: " app
# user="USER INPUT"
read -p "Enter Input for the app: " input



~/Downloads/grpcurl --plaintext -d '{"input": "'${input}'", "filePath": "'${app}'"}' localhost:10000 PuppetMasterService/runApplication