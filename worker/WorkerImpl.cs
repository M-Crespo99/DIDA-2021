using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using static DIDAWorkerService;
using System.Reflection;
using System.IO;
using System;
using DIDAWorker;

namespace worker
{
    public class WorkerImpl : DIDAWorkerServiceBase
    {
        DIDAStorageNode[] storageReplicas;
        delLocateStorageId locationFunction;
        public override Task<DIDAReply> workOnOperator(DIDARequest request, ServerCallContext context)
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
                            
                            var metaRecord = convertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;
                            
                            operatorFromReflection.ConfigureStorage(storageReplicas, locationFunction);
                            string newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                            
                            request.Chain[request.Next].Output = newOutput;
                            request.Meta = convertToProtoMetaRecord(metaRecord);
                            request.Next++;
                            if (request.Next < request.ChainSize)
                            {
                                var nextWorkerAssignment = request.Chain[request.Next];
                                GrpcChannel channel = GrpcChannel.ForAddress("http://" + nextWorkerAssignment.Host + ":" + nextWorkerAssignment.Port);
                                //var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);
                            }
                            
                        }
                    }
                }
            }
            return base.workOnOperator(request, context);
        }

        private DIDAWorker.DIDAMetaRecord convertToWorkerMetaRecord(DIDAMetaRecord metaRecord)
        {
            return new DIDAWorker.DIDAMetaRecord()
            {
                id = metaRecord.Id
            };
        }

        private DIDAMetaRecord convertToProtoMetaRecord(DIDAWorker.DIDAMetaRecord metaRecord)
        {
            return new DIDAMetaRecord()
            {
                Id = metaRecord.id
            };
        }
    }
}