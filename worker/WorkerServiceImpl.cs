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
using System.Security.Cryptography;
using System.Text;
using DIDARequest = DIDAWorker.Proto.DIDARequest;

namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerService.DIDAWorkerServiceBase
    {
        private String workerId;
        private int gossipDelay;

        List<DIDAStorageNode> storageReplicas = new List<DIDAStorageNode>();


        private int operatorCounter = 0;

        //First value of Tuple = number of executions, Second number is total elapsed time
        private ConcurrentDictionary<String, Tuple<int, int>> operatorDictionary = new ConcurrentDictionary<string, Tuple<int, int>>();

        private bool isDebug = false;
        private String debugHost = "";
        private int debugPort = 0;

        private int _totalTime = 0;

        public WorkerServiceImpl(String id, int gossipDelay)
        {
            this.workerId = id;
            this.gossipDelay = gossipDelay;
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
            string dllDirectory = String.Format("{0}/Operators/", argument);

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
                            IDIDAOperator operatorFromReflection = (IDIDAOperator)Activator.CreateInstance(type);

                            var metaRecord = ConvertToWorkerMetaRecord(request.Meta);
                            string previousOutput = request.Next == 0 ? "" : request.Chain[request.Next - 1].Output;

                            Stopwatch stopwatch = new Stopwatch();
                            string newOutput = "";
                            StorageProxy proxy = new StorageProxy(this.storageReplicas, metaRecord);
                            try
                            {
                                stopwatch.Start();
                                operatorFromReflection.ConfigureStorage(proxy);
                                newOutput = operatorFromReflection.ProcessRecord(metaRecord, request.Input, previousOutput);
                                stopwatch.Stop();

                            }
                            catch (RpcException e)
                            {
                                Console.WriteLine(e.Message);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("ERROR EXECUTING OPERATOR: ");
                                Console.WriteLine(e.ToString());
                            }

                            operatorCounter++;
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
                                System.Threading.Thread.Sleep(gossipDelay);
                                var nextWorkerAssignment = request.Chain[request.Next];
                                GrpcChannel channel = GrpcChannel.ForAddress("http://" + nextWorkerAssignment.Host + ":" + nextWorkerAssignment.Port);
                                var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);
                                var newRequest = new DIDARequest(request);
                                this.addNewMetaData(newRequest, proxy._meta);
                                Console.WriteLine("REQUEST: ");

                                Console.WriteLine(newRequest.ToString());

                                Console.WriteLine("################");

                                _ = client.workOnOperatorAsync(newRequest);
                            }
                        }
                    }
                }
            }

            if (!foundDLL)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: Could not locate operator: " + className);
                Console.ResetColor();
            }

            return await Task.FromResult(new DIDAReply());
        }


        private void addNewMetaData(DIDARequest request, DIDAMeta meta){
            foreach(var k in meta._keys){
                if(request.Meta.AccessedKeys.Contains(k)){
                    var index = meta._keys.IndexOf(k);
                    request.Meta.AccesedVersions[index] = new DIDAWorker.Proto.DIDAVersion{
                        VersionNumber = meta._versions[index].VersionNumber,
                        ReplicaId = meta._versions[index].ReplicaId
                    };
                }else{
                    var index = meta._keys.IndexOf(k);
                    request.Meta.AccessedKeys.Add(k);
                    request.Meta.AccesedVersions.Add(new DIDAWorker.Proto.DIDAVersion{
                        VersionNumber = meta._versions[index].VersionNumber,
                        ReplicaId = meta._versions[index].ReplicaId
                    });
                }
            }
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

            lock (this)
            {
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
            client.receiveDebugInfoAsync(new DebugInfoRequest { Info = resultString });
        }

        public async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Liveness check for worker##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new LivenessCheckReply { Ok = true });
        }

        private DIDAMeta ConvertToWorkerMetaRecord(DIDAWorker.Proto.DIDAMetaRecord metaRecord)
        {
            return new DIDAMeta(metaRecord);
        }

        private DIDAWorker.Proto.DIDAMetaRecord ConvertToProtoMetaRecord(DIDAMeta meta)
        {
            DIDAWorker.Proto.DIDAMetaRecord protoMetaRecord = new DIDAWorker.Proto.DIDAMetaRecord();
            protoMetaRecord.Id = meta.Id;

            foreach(var v in meta._versions){
                protoMetaRecord.AccesedVersions.Add(new DIDAWorker.Proto.DIDAVersion{
                    VersionNumber = v.VersionNumber,
                    ReplicaId = v.ReplicaId
                });
            }

            foreach(var k in meta._keys){
                protoMetaRecord.AccessedKeys.Add(k);
            }

            return protoMetaRecord;
        }
        public override Task<DebugReply> debug(DebugRequest request, ServerCallContext context)
        {
            lock (this)
            {
                isDebug = true;
                debugHost = request.Host;
                debugPort = request.Port;
            }

            return Task.FromResult(new DebugReply { Ok = true });
        }

        public override Task<StatusReply> status(StatusRequest request, ServerCallContext context)
        {
            printStatusOnConsole();
            return Task.FromResult<StatusReply>(new StatusReply { Ok = true });
        }

        public override Task<ListServerReply> listServer(ListServerRequest request, ServerCallContext context)
        {
            printStatusOnConsole();
            var operatorArray = operatorDictionary.Select(op => new DIDAWorkerListDetails { OperatorName = op.Key, Executions = op.Value.Item1, TotalTime = op.Value.Item2 }).ToArray();
            return Task.FromResult(new ListServerReply { Details = { operatorArray } });
        }

        private void printStatusOnConsole()
        {
            Console.WriteLine("-----------------------");
            Console.WriteLine("Worker {0} STATUS", this.workerId);
            Console.WriteLine("Total Time Spent working: {0}", this._totalTime);
            Console.WriteLine("Number of operators executed: " + operatorCounter);
            foreach (var op in operatorDictionary)
            {
                Console.WriteLine("Operator {0} was executed {1} times with an average computation time of {2}", op.Key, op.Value.Item1, ((float)op.Value.Item2) / op.Value.Item1);
            }
            Console.WriteLine("-----------------------");
        }
    }

    public class StorageProxy : IDIDAStorage
    {

        StorageFrontend.StorageFrontend _frontend;

        DIDAStorageNode _storageNode;

        List<DIDAStorageNode> _storageNodes;

        public DIDAMeta _meta;
        public StorageProxy(List<DIDAStorageNode> storageNodes, DIDAMeta meta)
        {
            this._storageNodes = storageNodes;
            this._meta = meta;
        }

        //Knuth hash.
        private ulong getHash(string str)
        {
            {
                UInt64 hashedValue = 3074457345618258791ul;
                for (int i = 0; i < str.Length; i++)
                {
                    hashedValue += str[i];
                    hashedValue *= 3074457345618258799ul;
                }
                return hashedValue;
            }
        }
        private DIDAStorageNode locateFunction(string idToRead)
        {


            var hash = this.getHash(idToRead);

            var node = this.getStorageFromHash(hash);

            return new DIDAWorker.DIDAStorageNode
            {
                host = node.host,
                serverId = node.serverId,
                port = node.port
            };
        }

        private DIDAStorageNode getStorageFromHash(ulong hash)
        {
            DIDAStorageNode closest = this._storageNodes.First();

            foreach (DIDAStorageNode node in this._storageNodes)
            {
                ulong hashOfNode = this.getHash(String.Format("{0}:{1}", node.host, node.port));

                if (hashOfNode > hash && hashOfNode < this.getHash(String.Format("{0}:{1}", closest.host, closest.port)))
                {
                    closest = node;
                }
            }
            return closest;
        }
        public DIDAWorker.DIDAVersion write(DIDAWriteRequest request)
        {
            this._storageNode = this.locateFunction(request.Id);

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port, this._storageNodes.Count);



            var reply = this._frontend.Write(request.Id, request.Val);


            this._meta.addAccessed(request.Id, reply.VersionNumber, reply.ReplicaId);



            return new DIDAWorker.DIDAVersion
            {
                VersionNumber = reply.VersionNumber,
                ReplicaId = reply.ReplicaId
            };
        }

        public DIDAWorker.DIDAVersion updateIfValueIs(DIDAUpdateIfRequest request)
        {
            this._storageNode = this.locateFunction(request.Id);

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port, this._storageNodes.Count);

            var reply = this._frontend.UpdateIfValueIs(request.Id, request.Oldvalue, request.Newvalue);

            if (reply != null)
            {
                return new DIDAWorker.DIDAVersion
                {
                    VersionNumber = reply.VersionNumber,
                    ReplicaId = reply.ReplicaId
                };

            }
            else
            {
                return new DIDAWorker.DIDAVersion
                {
                    VersionNumber = -1,
                    ReplicaId = -1
                };
            }

        }

        public DIDARecordReply read(DIDAReadRequest request)
        {
            this._storageNode = this.locateFunction(request.Id);

            this._frontend = new StorageFrontend.StorageFrontend(this._storageNode.host, this._storageNode.port, this._storageNodes.Count);

            var lastVersion = this._meta.getLastAccessedVersion(request.Id);

            DIDAStorage.Proto.DIDARecordReply reply;

            if(lastVersion.VersionNumber == -1){
                reply = this._frontend.Read(request.Id, request.Version.VersionNumber, request.Version.ReplicaId);
            }else{
                reply = this._frontend.Read(request.Id, lastVersion.VersionNumber, lastVersion.ReplicaId);
            }


            if (reply == null)
            {
                return new DIDAWorker.DIDARecordReply
                {
                    Version = new DIDAWorker.DIDAVersion
                    {
                        VersionNumber = -1,
                        ReplicaId = -1
                    }
                };
            }

            this._meta.addAccessed(request.Id, reply.Version.VersionNumber, reply.Version.ReplicaId);

            return new DIDAWorker.DIDARecordReply
            {
                Id = reply.Id,
                Version = new DIDAWorker.DIDAVersion
                {
                    VersionNumber = reply.Version.VersionNumber,
                    ReplicaId = reply.Version.ReplicaId
                },
                Val = reply.Val
            };
        }
    }

    public class DIDAMeta : DIDAWorker.DIDAMetaRecord
    {
        public List<DIDAWorker.DIDAVersion> _versions = new List<DIDAWorker.DIDAVersion>();
        public List<string> _keys = new List<string>();

        public DIDAMeta(DIDAWorker.Proto.DIDAMetaRecord meta)
        {
            this.Id = meta.Id;
            foreach (var v in meta.AccesedVersions)
            {
                this._versions.Add(new DIDAWorker.DIDAVersion
                {
                    VersionNumber = v.VersionNumber,
                    ReplicaId = v.ReplicaId
                });
            }
            foreach (var k in meta.AccessedKeys)
            {
                this._keys.Add(k);
            }
        }

        public void addAccessed(string id, int vNumber, int rId)
        {
            if (!this._keys.Contains(id))
            {
                this._keys.Add(id);
                this._versions.Add(new DIDAWorker.DIDAVersion
                {
                    VersionNumber = vNumber,
                    ReplicaId = rId
                });
            }
            else
            {
                this._versions[this._keys.IndexOf(id)] = new DIDAWorker.DIDAVersion
                {
                    VersionNumber = vNumber,
                    ReplicaId = rId
                };
            }
        }


        public DIDAWorker.DIDAVersion getLastAccessedVersion(string id){
            if(this._keys.Contains(id)){
                return this._versions[this._keys.IndexOf(id)];
            }else{
                return new DIDAWorker.DIDAVersion{
                    VersionNumber = -1,
                    ReplicaId = -1
                };
            }
        }
    }
}