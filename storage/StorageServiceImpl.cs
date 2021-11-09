using System.Threading.Tasks;
using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Grpc.Net.Client;
using System.Linq;


namespace storage{

    struct StorageDetails{

        public StorageDetails(String host, int port)
        {
            Host = host;
            Port = port;
        }
        public string Host { get; set;}
        public int Port { get; set;}
    }
    
    public class StorageServerService : DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceBase{
        
        DIDAStorage.Storage storage;

        ConcurrentDictionary<string, StorageDetails> _otherStorageNodes = new ConcurrentDictionary<string, StorageDetails>();


        List<GossipLib.GossipLogRecord> replicaLog = new List<GossipLib.GossipLogRecord>();

        List<int> executedUpdates = new List<int>();

        private int _gossipDelay;

        private string _host;
        private int _port;

        private int _replicaId;
        public StorageServerService(int replicaId, string host, int port, int gossipDelay){
            storage = new DIDAStorage.Storage(replicaId, true);
            this._gossipDelay = gossipDelay;

            this._host = host;
            this._port = port;
            this._replicaId = replicaId;
        }


        public override Task<DIDAStorage.Proto.DIDARecordReply> read(DIDAStorage.Proto.DIDAReadRequest request, ServerCallContext context){
            return Task.FromResult(processReadRequest(request));
        }

        public override Task<DIDAStorage.Proto.DIDAVersion> write(DIDAStorage.Proto.DIDAWriteRequest request, ServerCallContext context){
            return Task.FromResult(processWriteRequest(request));
        }

        public override Task<DIDAStorage.Proto.DIDAVersion> updateIfValueIs(DIDAStorage.Proto.DIDAUpdateIfRequest request, ServerCallContext context){
            return Task.FromResult(processUpdateIfRequest(request));
        }

        public override Task<DIDAStorage.Proto.DIDACrashServerReply> crashServer(DIDAStorage.Proto.DIDACrashServerRequest request, ServerCallContext context){
            Environment.Exit(1);
            //This should not reach here, but who knows
            throw new NotImplementedException();
        }

        public override Task<DIDAStorage.Proto.DIDAListServerReply> listServer(DIDAStorage.Proto.DIDAListServerRequest request, ServerCallContext context){
            return Task.FromResult(this.storage.getProtoRecords());
        }

        public override Task<DIDAStorage.Proto.ToggleDebugReply> toggleDebug(DIDAStorage.Proto.ToggleDebugRequest request, ServerCallContext context){
            bool debug = this.storage.toggleDebug();
            return Task.FromResult(new DIDAStorage.Proto.ToggleDebugReply{ DebugStatus = debug});
        }
        public override Task<DIDAStorage.Proto.StatusReply> status(DIDAStorage.Proto.StatusRequest request, ServerCallContext context){
            this.storage.printStatus();
            return Task.FromResult(new DIDAStorage.Proto.StatusReply{ Ok = true});
        }

        public override Task<DIDAStorage.Proto.GossipReply> gossip(DIDAStorage.Proto.GossipMessage request, ServerCallContext context){
            //Merge the logs
            //Merge incoming replica
            //Apply any updates that have become stable and havent been executed
            //write(id, value, version, )
            //Eliminate records from a log 
            //Keep gossiping


            this.mergeLogs(request);

            foreach(var entry in this.replicaLog){
                Console.WriteLine("LOG: " + entry.ToString());
            }

            try{
                var keysInLog = this.getKeysInReplicaLog();
                foreach(var key in keysInLog){
                    var records = this.filterLogByKey(key);
                    var orderedRecords = records.OrderBy(record => record._prev);
                    
                    foreach(var record in orderedRecords){
                        if(isStable(record) && !this.executedUpdates.Contains(record._operationIdentifier)){
                            this.storage.Write(record._operation.key, record._operation.newValue, record, true);
                            this.executedUpdates.Add(record._operationIdentifier);
                        }
                    }
                }
            }catch (Exception e){
                Console.WriteLine(e.ToString());
            }

            return Task.FromResult(new DIDAStorage.Proto.GossipReply{});
        }

        public override Task<DIDAStorage.Proto.AddStorageReply> addStorage(DIDAStorage.Proto.AddStorageRequest request, ServerCallContext context){


            //If it is a new storage
            if(!this._otherStorageNodes.ContainsKey(request.Id)){
                this.storage.incrementStorages();
            }

            if(!(this._otherStorageNodes.TryAdd(request.Id, new StorageDetails(request.Host, request.Port)))){
                this._otherStorageNodes[request.Id] =  new StorageDetails(request.Host, request.Port);
            }

            return Task.FromResult(new DIDAStorage.Proto.AddStorageReply{ Ok = true });
        }

        private DIDAStorage.Proto.DIDARecordReply processReadRequest(DIDAStorage.Proto.DIDAReadRequest request){
            try{
                DIDAStorage.DIDAVersion version = new DIDAStorage.DIDAVersion();

                //If version comes as null, we go for the most recent version. DIDAVersion is non nullable so putting -1 was the soltuion
                if(request.Version == null){
                    version.versionNumber = -1;
                    version.replicaId = -1;
                }
                else{
                    version = new DIDAStorage.DIDAVersion{
                                    versionNumber = request.Version.VersionNumber,
                                    replicaId = request.Version.ReplicaId
                                    };
                }
                DIDAStorage.DIDARecord record = storage.Read(request.Id, version);

                DIDAStorage.Proto.DIDARecordReply reply = new DIDAStorage.Proto.DIDARecordReply{
                    Id = request.Id,
                    Version = new DIDAStorage.Proto.DIDAVersion{
                        VersionNumber = record.version.versionNumber,
                        ReplicaId = record.version.replicaId,
                        Clock = LClockToProto(record.valueTS)
                    },
                    Val = record.val
                };
                return reply;

            }
            catch(DIDAStorage.Exceptions.DIDAStorageException e){
                throw new RpcException(new Status(StatusCode.InvalidArgument, e.ToString()));
            }
        }

         private DIDAStorage.Proto.DIDAVersion processWriteRequest(DIDAStorage.Proto.DIDAWriteRequest request){
            //Check if new update
            //If new, increment ith element of replica timestamp
            //Assign new timestamp to u
            //Put u on the log
            //If update is stable, process the update
            //Process: value = apply(value, operation)
            //         valueTS = merge(valueTS and updateTS)
            //         Append to executed updates


            
            var newEntry = this.addToLog(request);

            try{
                foreach(var entry in this.replicaLog){
                Console.WriteLine("LOG: " + entry.ToString());
            }
            } catch (Exception e){
                Console.WriteLine(e.ToString());
            }
            

            DIDAStorage.DIDAVersion version;
            try{
                if(this.isStable(newEntry)){
                     version = storage.Write(request.Id, request.Val, newEntry, false);
                     this.executedUpdates.Add(request.UniqueID);
                     this.sendGossipMessages();
                }else{
                    version = new DIDAStorage.DIDAVersion{
                        versionNumber = -1,
                        replicaId = this._replicaId,
                        replicaTS = newEntry._replicaTS
                    };
                }
            return new DIDAStorage.Proto.DIDAVersion {
                VersionNumber = version.versionNumber,
                ReplicaId = version.replicaId,
                Clock = LClockToProto(version.replicaTS)
            };
            }catch (Exception e){
                Console.WriteLine(e.ToString());
                return new DIDAStorage.Proto.DIDAVersion();
            }

        }

        private DIDAStorage.Proto.DIDAVersion processUpdateIfRequest(DIDAStorage.Proto.DIDAUpdateIfRequest request){
            try{
                DIDAStorage.DIDAVersion version = storage.UpdateIfValueIs(request.Id, request.Oldvalue, request.Newvalue);

                if(version.versionNumber < 0){
                     throw new RpcException(new Status(StatusCode.InvalidArgument, "Value to update did not match."));
                }

                return new DIDAStorage.Proto.DIDAVersion {
                VersionNumber = version.versionNumber,
                ReplicaId = version.replicaId,
            };
            }catch(DIDAStorage.Exceptions.DIDAStorageException e){
                throw new RpcException(new Status(StatusCode.InvalidArgument, e.ToString()));
            }
        }
        private DIDAStorage.Proto.LamportClock LClockToProto(GossipLib.LamportClock c){
            DIDAStorage.Proto.LamportClock protoLClock = new DIDAStorage.Proto.LamportClock();

            var l = c.toList();

            foreach(var value in l){
                protoLClock.Values.Add(value);
            }

            return protoLClock;
        }

        private GossipLib.LamportClock protoToLClock(DIDAStorage.Proto.LamportClock protoClock){
            List<int> l = new List<int>();

            foreach(var value in protoClock.Values){
                l.Add(value);
            }

            return new GossipLib.LamportClock(l);
        }

        private GossipLib.GossipLogRecord addToLog(DIDAStorage.Proto.DIDAWriteRequest request){
            try{

            
            int replicaId = this._replicaId;
            
            //increment ReplicaTS at replicaID by one
            this.storage.incrementReplicaTSOnRecord(request.Id);

            var prev = this.protoToLClock(request.Clock);
            
            var ts = prev;

            ts.incrementAt(this._replicaId - 1);

            var updateId = request.UniqueID;

            var op = new GossipLib.operation{
                key = request.Id,
                opType = GossipLib.operationType.WRITE,
                versionNumber = -1,
                newValue = request.Val
            };

            var replicaTS = this.storage.getReplicaTimestamp(request.Id).DeepCopy();


            var newEntry = new GossipLib.GossipLogRecord(replicaId,
                                                        ts,
                                                        this.protoToLClock(request.Clock),
                                                        replicaTS,
                                                        updateId,
                                                        op);

            this.replicaLog.Add(newEntry);

            return newEntry;
                        
            }     
            catch (Exception e){
                Console.WriteLine(e.ToString());
            }
            return null;
        }


        private bool isStable(GossipLib.GossipLogRecord record){
            try{
                return record._prev <= this.storage.getValueTS(record._operation.key);

            } catch(Exception e){
                Console.WriteLine(e.ToString());
            }
            return false;
        }


        private void sendGossipMessages(){
            foreach(var storage in this._otherStorageNodes){
                InternalStorageFrontend f = new InternalStorageFrontend(storage.Value.Host, storage.Value.Port);
                f.gossip(this.replicaLog);
            }
        }

        private void mergeLogs(DIDAStorage.Proto.GossipMessage request){
            foreach(var entry in request.Log){
                var logEntry = ProtoGRecordToGRecord(entry);
                var replicaTS = this.storage.getReplicaTimestamp(entry.Operation.Key);
                if((this.replicaLog.Find(e => e._operationIdentifier == logEntry._operationIdentifier) == null)
                && !(protoToLClock(entry.UpdateTS) <= replicaTS)){
                    this.replicaLog.Add(logEntry);
                    this.storage.getReplicaTimestamp(entry.Operation.Key).merge(protoToLClock(entry.ReplicaTS));
                }
            }
        }
        private DIDAStorage.Proto.GossipLogEntry GRecordToGProtoRecord(GossipLib.GossipLogRecord entry){
            var protoEntry = new DIDAStorage.Proto.GossipLogEntry{
                ReplicaID = entry._replicaId,
                UpdateTS = LClockToProto(entry._updateTS),
                PreviousClock = LClockToProto(entry._prev),
                UpdateIdentifier = entry._operationIdentifier,
                Operation = new DIDAStorage.Proto.GossipOperation{
                    Key = entry._operation.key,
                    VersionNumber = entry._operation.versionNumber,
                    NewValue = entry._operation.newValue
                },

                ReplicaTS = LClockToProto(entry._replicaTS)
            };
            return protoEntry;
        }

        private GossipLib.GossipLogRecord  ProtoGRecordToGRecord(DIDAStorage.Proto.GossipLogEntry entry){
            var newEntry = new GossipLib.GossipLogRecord(
                entry.ReplicaID,
                protoToLClock(entry.UpdateTS),
                protoToLClock(entry.PreviousClock),
                protoToLClock(entry.ReplicaTS),
                entry.UpdateIdentifier,
                new GossipLib.operation{
                    key = entry.Operation.Key,
                    versionNumber = entry.Operation.VersionNumber,
                    newValue = entry.Operation.NewValue
                }
                
            );
            return newEntry;
        }
        private List<string> getKeysInReplicaLog(){
            var listToReturn = new List<string>();

            foreach(var record in this.replicaLog){
                if(!listToReturn.Contains(record._operation.key)){
                    listToReturn.Add(record._operation.key);
                }
            }
            return listToReturn;
        }

        private List<GossipLib.GossipLogRecord> filterLogByKey(string key){
            var listToReturn = new List<GossipLib.GossipLogRecord>();
            foreach(var logRecord in this.replicaLog){
                if(logRecord._operation.key == key){
                    listToReturn.Add(logRecord);
                }
            }
            return listToReturn;
        }
    }

        public class InternalStorageFrontend
    {
        
        private int _port;

        private string _host;
        private GrpcChannel _channel;
        private DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient _client;

        private string _lastErrorMessage = "";

        private bool _verbose = false;


        public InternalStorageFrontend(string host, int port)
        {
            this._port = port;
            this._host = host;


            this._verbose = false;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);

        }

        public InternalStorageFrontend(string host, int port, bool verbose)
        {
            this._port = port;
            this._host = host;



            this._verbose = verbose;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);

        }
        public string getLastErrorMessage(){
            return this._lastErrorMessage;
        }

        public void ToggleVerbose(){
            this._verbose = !this._verbose;
        }


        public void gossip(List<GossipLib.GossipLogRecord> records){

            var gossipMessage = new DIDAStorage.Proto.GossipMessage();

            foreach(var record in records){
                var newProtoEntry = GRecordToGProtoRecord(record);


                gossipMessage.Log.Add(newProtoEntry);
            }

            _ = this._client.gossipAsync(gossipMessage);

            return;
        }        

        private GossipLib.LamportClock protoToLClock(DIDAStorage.Proto.LamportClock protoClock){
            List<int> l = new List<int>();

            foreach(var value in protoClock.Values){
                l.Add(value);
            }

            return new GossipLib.LamportClock(l);
        }

        private DIDAStorage.Proto.LamportClock LClockToProto(GossipLib.LamportClock c){
            DIDAStorage.Proto.LamportClock protoLClock = new DIDAStorage.Proto.LamportClock();

            var l = c.toList();

            foreach(var value in l){
                protoLClock.Values.Add(value);
            }

            return protoLClock;
        }

        private DIDAStorage.Proto.GossipLogEntry GRecordToGProtoRecord(GossipLib.GossipLogRecord entry){
            var protoEntry = new DIDAStorage.Proto.GossipLogEntry{
                ReplicaID = entry._replicaId,
                UpdateTS = LClockToProto(entry._updateTS),
                PreviousClock = LClockToProto(entry._prev),
                UpdateIdentifier = entry._operationIdentifier,
                Operation = new DIDAStorage.Proto.GossipOperation{
                    Key = entry._operation.key,
                    NewValue = entry._operation.newValue
                },

                ReplicaTS = LClockToProto(entry._replicaTS)
            };
            return protoEntry;
        }
    }

    
}