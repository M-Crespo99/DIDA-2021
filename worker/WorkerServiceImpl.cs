using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using System.Reflection;
using System.IO;
using System;
using DIDAWorker;
using DIDAWorker.Proto;

namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerService.DIDAWorkerServiceBase
    {
        DIDAStorageNode[] storageReplicas;
        delLocateStorageId locationFunction;
        public async override Task<DIDAReply> workOnOperator(DIDAWorker.Proto.DIDARequest request, ServerCallContext context)
        {
            string className = request.Chain[request.Next].Operator.Classname;
            string dllNameTermination = ".dll";
            string currWorkingDir = Directory.GetCurrentDirectory();
            IDIDAOperator operatorFromReflection;

            foreach (string filename in Directory.EnumerateFiles(currWorkingDir))
            {
                if (filename.EndsWith(dllNameTermination))
                {
                    Assembly dll = Assembly.LoadFrom(filename);
                    Type[] typeList = dll.GetTypes();
                    foreach (Type type in typeList)
                    {
                        if (type.Name == className)
                        {
                            operatorFromReflection = (IDIDAOperator) Activator.CreateInstance(type);
                            
                            var metaRecord = ConvertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;
                            
                            operatorFromReflection.ConfigureStorage(storageReplicas, locationFunction);
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