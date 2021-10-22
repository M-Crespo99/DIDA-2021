using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using System.Reflection;
using System.IO;
using System;
using System.Collections.Generic;
using DIDAWorker;
using DIDAWorker.Proto;
using System.Linq;
namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerService.DIDAWorkerServiceBase
    {
        List<DIDAStorageNode> storageReplicas = new List<DIDAStorageNode>();

        delLocateStorageId locationFunction;

        private int operatorCounter = 0;

        public WorkerServiceImpl(){
            this.locationFunction = new delLocateStorageId(this.locateStorage);
        }

        public async override Task<DIDAReply> workOnOperator(DIDAWorker.Proto.DIDARequest request, ServerCallContext context)
        {
            lock(this){
                operatorCounter++;
            }
            List<DIDAStorageNode> newStorages = new List<DIDAStorageNode>();            
            foreach (var storageNode in request.Meta.Storages)
            {
                var node = new DIDAStorageNode();
                node.serverId = storageNode.Id;
                node.host = storageNode.Host;
                node.port = storageNode.Port;
                newStorages.Add(node);
            }
            this.storageReplicas = newStorages;

            string className = request.Chain[request.Next].Operator.Classname;

            string dllNameTermination = ".dll";
            string currWorkingDir = Directory.GetCurrentDirectory();

            
            var argument = Environment.CurrentDirectory.
                    Replace("PuppetMaster", "worker");
            string dllDirectory =  String.Format("{0}/Operators/", argument);
            
            bool foundDLL = false;

            foreach (string filename in Directory.EnumerateFiles(dllDirectory))
            {
                if (filename.EndsWith(dllNameTermination))
                {
                    Assembly dll = Assembly.LoadFrom(filename);
                    Type[] typeList = dll.GetTypes();
                    foreach (Type type in typeList)
                    {   

                        if (type.Name == className)
                        {
                            foundDLL = true;
                            IDIDAOperator operatorFromReflection = (IDIDAOperator) Activator.CreateInstance(type);
                            
                            var metaRecord = ConvertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;
                            

                            string newOutput = "";
                            try{
                                operatorFromReflection.ConfigureStorage(storageReplicas.ToArray(), locationFunction);                            
                                newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                            }catch(RpcException e){
                                Console.WriteLine(e.Message);
                            }
                            catch(Exception e){
                                Console.WriteLine(e.ToString());
                            }
                            
                            request.Chain[request.Next].Output = newOutput;
                            request.Next++;
                            if (request.Next < request.ChainSize)
                            {
                                var nextWorkerAssignment = request.Chain[request.Next];
                                GrpcChannel channel = GrpcChannel.ForAddress("http://" + nextWorkerAssignment.Host + ":" + nextWorkerAssignment.Port);
                                var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);
                                var newRequest = new DIDAWorker.Proto.DIDARequest(request);

                                client.workOnOperatorAsync(newRequest);
                            }
                        }
                    }
                }
            }

            if(!foundDLL){
                Console.WriteLine("ERROR: Could not locate operator: " + className);
            }

            return await Task.FromResult(new DIDAReply());
        }
        
        public override async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Liveness check for worker##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new LivenessCheckReply{Ok = true});
        }

        private DIDAWorker.DIDAMetaRecord ConvertToWorkerMetaRecord(DIDAWorker.Proto.DIDAMetaRecord metaRecord)
        {
            return new DIDAWorker.DIDAMetaRecord()
            {
                id = metaRecord.Id
            };
        }

        private DIDAWorker.Proto.DIDAMetaRecord ConvertToProtoMetaRecord(DIDAWorker.DIDAMetaRecord metaRecord)
        {
            return new DIDAWorker.Proto.DIDAMetaRecord()
            {
                Id = metaRecord.id
                
            };
        }

        private DIDAWorker.DIDAStorageNode locateStorage(string id, DIDAWorker.OperationType type){
            //This is temporary. There is only one storage with id = 1, for now.
            return new DIDAWorker.DIDAStorageNode{
                serverId = "1",
            };
        }
    }
}