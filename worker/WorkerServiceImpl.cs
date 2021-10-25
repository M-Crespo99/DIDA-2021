using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using System.Reflection;
using System.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using DIDAWorker;
using DIDAWorker.Proto;
using System.Linq;
using DIDARequest = DIDAWorker.Proto.DIDARequest;

namespace worker
{
    public class WorkerServiceImpl: DIDAWorkerService.DIDAWorkerServiceBase
    {
        List<DIDAStorageNode> storageReplicas = new List<DIDAStorageNode>();

        delLocateStorageId locationFunction;

        private int operatorCounter = 0;

        //First value of Tuple = number of executions, Second number is total elapsed time
        private ConcurrentDictionary<String, Tuple<int, int>> operatorDictionary = new ConcurrentDictionary<string, Tuple<int, int>>();

        private bool isDebug = false;
        private String debugHost = "";
        private int debugPort = 0;

        public WorkerServiceImpl(){
            this.locationFunction = new delLocateStorageId(this.locateStorage);
        }

        public async override Task<DIDAReply> workOnOperator(DIDAWorker.Proto.DIDARequest request, ServerCallContext context)
        {
            SetupStorage(request);

            bool foundDLL = false;
            string className = request.Chain[request.Next].Operator.Classname;

            string dllNameTermination = ".dll";
            string currWorkingDir = Directory.GetCurrentDirectory();
            var argument = Environment.CurrentDirectory.
                    Replace("PuppetMaster", "worker").Replace("PCS", "worker");
            string dllDirectory =  String.Format("{0}/Operators/", argument);

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
                            Console.WriteLine("Found the operator: " + className);

                            foundDLL = true;
                            IDIDAOperator operatorFromReflection = (IDIDAOperator) Activator.CreateInstance(type);
                            
                            var metaRecord = ConvertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;
                            
                            Stopwatch stopwatch = new Stopwatch();
                            string newOutput = "";
                            try
                            {
                                stopwatch.Start();
                                operatorFromReflection.ConfigureStorage(storageReplicas.ToArray(), locationFunction);                            
                                newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                                stopwatch.Stop();
                            }catch(RpcException e){
                                Console.WriteLine(e.Message);
                            }
                            catch(Exception e){
                                Console.WriteLine(e.ToString());
                            }

                            StoreOperatorInformationInDict(className, stopwatch);

                            if (isDebug)
                            {
                                logDebug(className, newOutput);
                            }

                            request.Chain[request.Next].Output = newOutput;
                            request.Next++;
                            if (request.Next < request.ChainSize)
                            {
                                var nextWorkerAssignment = request.Chain[request.Next];
                                GrpcChannel channel = GrpcChannel.ForAddress("http://" + nextWorkerAssignment.Host + ":" + nextWorkerAssignment.Port);
                                var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);
                                var newRequest = new DIDARequest(request);

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

        private void StoreOperatorInformationInDict(string className, Stopwatch stopwatch)
        {
            if (operatorDictionary.ContainsKey(className))
            {
                var currValue = operatorDictionary[className];
                operatorDictionary[className] = new Tuple<int, int>(currValue.Item1 + 1,
                    currValue.Item2 + stopwatch.Elapsed.Milliseconds);
            }
            else
            {
                operatorDictionary[className] = new Tuple<int, int>(1, stopwatch.Elapsed.Milliseconds);
            }
        }

        private void SetupStorage(DIDARequest request)
        {
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
        }

        private void logDebug(String classname, String output)
        {
            String resultString = String.Format("Operator {0} was executed with the output {1}", classname, output);
            GrpcChannel channel = GrpcChannel.ForAddress("http://" + debugHost + ":" + debugPort);
            var client = new PuppetMasterService.PuppetMasterServiceClient(channel);
            client.receiveDebugInfoAsync(new DebugInfoRequest {Info = resultString});
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

        public override Task<DebugReply> debug(DebugRequest request, ServerCallContext context)
        {
            lock (this)
            {
                isDebug = true;
                debugHost = request.Host;
                debugPort = request.Port;
            }

            return Task.FromResult(new DebugReply{Ok = true});
        }

        public override Task<StatusReply> status(StatusRequest request, ServerCallContext context)
        {
            Console.WriteLine("-----------------------");
            Console.WriteLine("Worker");
            Console.WriteLine("Number of operators executed: " + operatorCounter);
            foreach (var op in operatorDictionary)
            {
                Console.WriteLine("Operator {0} was executed {1} times with an average computation time of {2}", op.Key, op.Value.Item1, op.Value.Item2/op.Value.Item1 );
            }
            Console.WriteLine("-----------------------");
            return Task.FromResult<StatusReply>(new StatusReply{Ok = true});
        }

        public override Task<ListServerReply> listServer(ListServerRequest request, ServerCallContext context)
        {
            var operatorArray = operatorDictionary.Select(op => new DIDAWorkerListDetails{ OperatorName = op.Key, Executions = op.Value.Item1, TotalTime = op.Value.Item2}).ToArray();
            return Task.FromResult(new ListServerReply { Details = { operatorArray }});
        }
    }
}