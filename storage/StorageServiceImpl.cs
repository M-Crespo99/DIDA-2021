using System.Threading.Tasks;
using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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


        List<StorageFrontend.GossipLogRecord> replicaLog = new List<StorageFrontend.GossipLogRecord>();

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

            Console.WriteLine("Storage {0}:{1}.", _host, _port);
            
            Console.WriteLine("Other Known Storages: ");

            foreach(var entry in this._otherStorageNodes){
                Console.WriteLine("\tStorage {0} at {1}:{2}", entry.Key, entry.Value.Host, entry.Value.Port);
            }
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");

            

            return Task.FromResult(new DIDAStorage.Proto.AddStorageReply{ Ok = true });
        }

        private DIDAStorage.Proto.DIDARecordReply processReadRequest(DIDAStorage.Proto.DIDAReadRequest request){
            try{
                Console.WriteLine("READ -> " + request.Clock.ToString());
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

            Console.WriteLine("WRITE -> " + request.Clock.ToString());

            
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
                     version = storage.Write(request.Id, request.Val, newEntry._updateTS);
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
        private DIDAStorage.Proto.LamportClock LClockToProto(StorageFrontend.LamportClock c){
            DIDAStorage.Proto.LamportClock protoLClock = new DIDAStorage.Proto.LamportClock();

            var l = c.toList();

            foreach(var value in l){
                protoLClock.Values.Add(value);
            }

            return protoLClock;
        }

        private StorageFrontend.LamportClock protoToLClock(DIDAStorage.Proto.LamportClock protoClock){
            List<int> l = new List<int>();

            foreach(var value in protoClock.Values){
                l.Add(value);
            }

            return new StorageFrontend.LamportClock(l);
        }

        private StorageFrontend.GossipLogRecord addToLog(DIDAStorage.Proto.DIDAWriteRequest request){
            try{

            
            int replicaId = this._replicaId;
            
            //increment ReplicaTS at replicaID by one
            this.storage.incrementReplicaTSOnRecord(request.Id);

            var prev = this.protoToLClock(request.Clock);
            
            var ts = prev;

            ts.incrementAt(this._replicaId - 1);

            var updateId = request.UniqueID;

            var op = new StorageFrontend.operation{
                key = request.Id,
                opType = StorageFrontend.operationType.WRITE,
                newValue = request.Val
            };

            var replicaTS = this.storage.getReplicaTimestamp(request.Id).DeepCopy();


            var newEntry = new StorageFrontend.GossipLogRecord(replicaId,
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


        private bool isStable(StorageFrontend.GossipLogRecord record){
            return record._prev <= this.storage.getValueTS(record._operation.key);
        }


        private void sendGossipMessages(){
            foreach(var entry in this.replicaLog){
                
            }
        }
    }
}