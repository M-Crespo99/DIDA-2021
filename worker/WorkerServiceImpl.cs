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
using StorageFrontend;
using DIDARequest = DIDAWorker.Proto.DIDARequest;

namespace worker
{

    public class StorageProxy : IDIDAStorage{
        StorageFrontend.StorageFrontend _frontend;

        DIDAStorageNode _storageNode;
        public StorageProxy(){
            
        }

        private DIDAStorageNode locateFunction(string idToRead){
            return new DIDAWorker.DIDAStorageNode{
                host = "localhost",
                serverId = "1",
                port = 5001
            };
        }
        public DIDAWorker.DIDAVersion  write(DIDAWriteRequest request){
            this._storageNode = this.locateFunction("");

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port);

            var reply = this._frontend.Write(request.Id, request.Val);

            return new DIDAWorker.DIDAVersion{
                VersionNumber = reply.VersionNumber,
                ReplicaId = reply.ReplicaId
            };
        }

        public DIDAWorker.DIDAVersion updateIfValueIs(DIDAUpdateIfRequest request){
            this._storageNode = this.locateFunction("");

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port);

            var reply = this._frontend.UpdateIfValueIs(request.Id, request.Oldvalue, request.Newvalue);
            
            if(reply != null){
                return new DIDAWorker.DIDAVersion{
                    VersionNumber = reply.VersionNumber,
                    ReplicaId = reply.ReplicaId
                };

            }else{
                return new DIDAWorker.DIDAVersion{
                    VersionNumber = -1,
                    ReplicaId = -1
                }; 
            }

        }

        public DIDARecordReply read(DIDAReadRequest request){
            this._storageNode = this.locateFunction("");

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port);

            var reply = this._frontend.Read(request.Id, request.Version.VersionNumber, request.Version.ReplicaId);

            return new DIDAWorker.DIDARecordReply{
                Id = reply.Id,
                Version = new DIDAWorker.DIDAVersion{
                    VersionNumber = reply.Version.VersionNumber,
                    ReplicaId = reply.Version.ReplicaId
                },
                Val = reply.Val
            };
        }
    }
    public class WorkerServiceImpl : DIDAWorkerService.DIDAWorkerServiceBase
    {
        private String workerId;
        
        List<DIDAStorageNode> storageReplicas = new List<DIDAStorageNode>();


        private int operatorCounter = 0;

        //First value of Tuple = number of executions, Second number is total elapsed time
        private ConcurrentDictionary<String, Tuple<int, int>> operatorDictionary = new ConcurrentDictionary<string, Tuple<int, int>>();

        private bool isDebug = false;
        private String debugHost = "";
        private int debugPort = 0;

        private int _totalTime = 0;

        public WorkerServiceImpl(String id){
            this.workerId = id;
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
                            try{
                                Console.WriteLine("Going to storage");

                                StorageProxy proxy = new StorageProxy();
                                operatorFromReflection.ConfigureStorage(proxy);                            
                                newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                                stopwatch.Stop();
                                operatorCounter++;
                            }catch(RpcException e){
                                Console.WriteLine(e.Message);
                            }
                            catch(Exception e){
                                Console.WriteLine(e.ToString());
                            }

                            StoreOperatorInformationInDict(className, stopwatch);
                            SendOperatorInfoToScheduler(request.Meta.SchedulerHost, request.Meta.SchedulerPort, className, stopwatch);

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
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: Could not locate operator: " + className);
                Console.ResetColor();
            }

            return await Task.FromResult(new DIDAReply());
        }

        private void SendOperatorInfoToScheduler(String schedulerHost, int schedulerPort, String className, Stopwatch stopwatch)
        {
            GrpcChannel channel = GrpcChannel.ForAddress(String.Format("http://{0}:{1}", schedulerHost, schedulerPort));
            var client = new DIDASchedulerService.DIDASchedulerServiceClient(channel);
            client.operatorCompleteAsync(new CompleteOperatorRequest
            {
                OperationTime = stopwatch.Elapsed.Milliseconds,
                OperatorName = className,
                WorkerId = this.workerId
            });
        }
        
        private void StoreOperatorInformationInDict(string className, Stopwatch stopwatch)
        {
            int elapsedTime = stopwatch.Elapsed.Milliseconds;
            if (operatorDictionary.ContainsKey(className))
            {
                var currValue = operatorDictionary[className];
                operatorDictionary[className] = new Tuple<int, int>(currValue.Item1 + 1,
                    currValue.Item2 + elapsedTime);
            }
            else
            {
                operatorDictionary[className] = new Tuple<int, int>(1, elapsedTime);
            }

            lock(this){
                this._totalTime += elapsedTime;
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

        public async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
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
                Id = metaRecord.Id
            };
        }

        private DIDAWorker.Proto.DIDAMetaRecord ConvertToProtoMetaRecord(DIDAWorker.DIDAMetaRecord metaRecord)
        {
            return new DIDAWorker.Proto.DIDAMetaRecord()
            {
                Id = metaRecord.Id
                
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
            Console.WriteLine("Worker {0}", this.workerId);
            Console.WriteLine("Total Time Spent working: {0}", this._totalTime);
            Console.WriteLine("Number of operators executed: " + operatorCounter);
            foreach (var op in operatorDictionary)
            {
                Console.WriteLine("Operator {0} was executed {1} times with an average computation time of {2}", op.Key, op.Value.Item1, ((float) op.Value.Item2)/op.Value.Item1 );
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