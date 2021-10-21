using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using System.Reflection;
using System.IO;
using System;
using System.Collections.Generic;
using DIDAWorker;
using DIDAWorker.Proto;

namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerService.DIDAWorkerServiceBase
    {
        List<DIDAStorageNode> storageReplicas = new List<DIDAStorageNode>();
        delLocateStorageId locationFunction;

        public async override Task<DIDAReply> workOnOperator(DIDAWorker.Proto.DIDARequest request, ServerCallContext context)
        {
            Console.WriteLine("Running a work Operator");
            Console.WriteLine("WORKER: REQUEST: " + request.ToString());

            Console.WriteLine("WORKER: REQUEST: {" + request.Next+ " }");

            foreach (var storageNode in request.Meta.Storages)
            {
                var node = new DIDAStorageNode();

                node.serverId = storageNode.Id;
                node.host = storageNode.Host;
                node.port = storageNode.Port;

                storageReplicas.Add(node);
            }

            string className = request.Chain[request.Next].Operator.Classname;
            Console.WriteLine("WORKER: Afer Chain " + className);

            string dllNameTermination = ".dll";
            string currWorkingDir = Directory.GetCurrentDirectory();
            Console.WriteLine("WORKER currWorkingDir" + currWorkingDir);

            IDIDAOperator operatorFromReflection;
            var argument = Environment.CurrentDirectory.
                    Replace("PuppetMaster", "worker");
            Console.WriteLine("WORKER Argument" + argument);
            string dllDirectory =  String.Format("{0}/Operators/", argument);
            Console.WriteLine("WORKER: + " + dllDirectory);
            foreach (string filename in Directory.EnumerateFiles(dllDirectory))
            {
                if (filename.EndsWith(dllNameTermination))
                {
                    Console.WriteLine("WORKER: FOUND A DLL");
                    Assembly dll = Assembly.LoadFrom(filename);
                    Type[] typeList = dll.GetTypes();
                    foreach (Type type in typeList)
                    {
                        if (type.Name == className)
                        {
                            Console.WriteLine("WORKER: FOUND CLASS");
                            operatorFromReflection = (IDIDAOperator) Activator.CreateInstance(type);
                            
                            var metaRecord = ConvertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;
                            
                            Console.WriteLine("Configuring Storage");
                            
                            operatorFromReflection.ConfigureStorage(storageReplicas.ToArray(), locationFunction);
                            
                            Console.WriteLine("Processing Record");
                            string newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                            
                            request.Chain[request.Next].Output = newOutput;
                            request.Meta = ConvertToProtoMetaRecord(metaRecord);
                            request.Next++;
                            if (request.Next < request.ChainSize)
                            {
                                var nextWorkerAssignment = request.Chain[request.Next];
                                GrpcChannel channel = GrpcChannel.ForAddress("http://" + nextWorkerAssignment.Host + ":" + nextWorkerAssignment.Port);
                                var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);
                                client.workOnOperatorAsync(request);
                            }
                            
                        }
                    }
                }
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
    }
}