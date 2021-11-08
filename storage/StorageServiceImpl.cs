using System.Threading.Tasks;
using Grpc.Core;
using System;
using System.Collections.Concurrent;

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
        

        GossipLib.LamportClock _clock;
        DIDAStorage.Storage storage;

        ConcurrentDictionary<string, StorageDetails> _otherStorageNodes = new ConcurrentDictionary<string, StorageDetails>();

        private int _gossipDelay;

        private string _host;
        private int _port;
        public StorageServerService(int replicaId, string host, int port, int gossipDelay){
            storage = new DIDAStorage.Storage(replicaId, true);
            this._gossipDelay = gossipDelay;

            this._host = host;
            this._port = port;
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

        public override Task<DIDAStorage.Proto.AddStorageReply> addStorage(DIDAStorage.Proto.AddStorageRequest request, ServerCallContext context){
            
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
                        ReplicaId = record.version.replicaId
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
            DIDAStorage.DIDAVersion version = storage.Write(request.Id, request.Val);
            return new DIDAStorage.Proto.DIDAVersion {
                VersionNumber = version.versionNumber,
                ReplicaId = version.replicaId,
            };
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
    }
}