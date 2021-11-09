using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using DIDAStorage;
using DIDAStorage.Exceptions;
using DIDAStorage.Proto;
using Grpc.Core;
using DIDAVersion = DIDAStorage.Proto.DIDAVersion;
using LamportClock = GossipLib.LamportClock;

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
    
    public class StorageServerService : DIDAStorageService.DIDAStorageServiceBase{
        

        LamportClock _clock;
        Storage storage;

        ConcurrentDictionary<string, StorageDetails> _otherStorageNodes = new ConcurrentDictionary<string, StorageDetails>();

        private int _gossipDelay;

        private string _host;
        private int _port;
        public StorageServerService(int replicaId, string host, int port, int gossipDelay){
            storage = new Storage(replicaId, true);
            this._gossipDelay = gossipDelay;

            this._host = host;
            this._port = port;
        }


        public override Task<DIDARecordReply> read(DIDAReadRequest request, ServerCallContext context){
            return Task.FromResult(processReadRequest(request));
        }

        public override Task<DIDAVersion> write(DIDAWriteRequest request, ServerCallContext context){
            return Task.FromResult(processWriteRequest(request));
        }

        public override Task<DIDAVersion> updateIfValueIs(DIDAUpdateIfRequest request, ServerCallContext context){
            return Task.FromResult(processUpdateIfRequest(request));
        }

        public override Task<DIDACrashServerReply> crashServer(DIDACrashServerRequest request, ServerCallContext context){
            Environment.Exit(1);
            //This should not reach here, but who knows
            throw new NotImplementedException();
        }

        public override Task<DIDAListServerReply> listServer(DIDAListServerRequest request, ServerCallContext context){
            return Task.FromResult(this.storage.getProtoRecords());
        }

        public override Task<ToggleDebugReply> toggleDebug(ToggleDebugRequest request, ServerCallContext context){
            bool debug = this.storage.toggleDebug();
            return Task.FromResult(new ToggleDebugReply{ DebugStatus = debug});
        }
        public override Task<StatusReply> status(StatusRequest request, ServerCallContext context){
            this.storage.printStatus();
            return Task.FromResult(new StatusReply{ Ok = true});
        }

        public override Task<AddStorageReply> addStorage(AddStorageRequest request, ServerCallContext context){
            
            if(!(this._otherStorageNodes.TryAdd(request.Id, new StorageDetails(request.Host, request.Port)))){
                this._otherStorageNodes[request.Id] =  new StorageDetails(request.Host, request.Port);
            }

            Console.WriteLine("Storage {0}:{1}.", _host, _port);
            
            Console.WriteLine("Other Known Storages: ");

            foreach(var entry in this._otherStorageNodes){
                Console.WriteLine("\tStorage {0} at {1}:{2}", entry.Key, entry.Value.Host, entry.Value.Port);
            }
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");

            return Task.FromResult(new AddStorageReply{ Ok = true });
        }

        private DIDARecordReply processReadRequest(DIDAReadRequest request){
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
                DIDARecord record = storage.Read(request.Id, version);

                DIDARecordReply reply = new DIDARecordReply{
                    Id = request.Id,
                    Version = new DIDAVersion{
                        VersionNumber = record.version.versionNumber,
                        ReplicaId = record.version.replicaId
                    },
                    Val = record.val
                };
                return reply;

            }
            catch(DIDAStorageException e){
                throw new RpcException(new Status(StatusCode.InvalidArgument, e.ToString()));
            }
        }

         private DIDAVersion processWriteRequest(DIDAWriteRequest request){
            DIDAStorage.DIDAVersion version = storage.Write(request.Id, request.Val);
            return new DIDAVersion {
                VersionNumber = version.versionNumber,
                ReplicaId = version.replicaId,
            };
        }

        private DIDAVersion processUpdateIfRequest(DIDAUpdateIfRequest request){
            try{
                DIDAStorage.DIDAVersion version = storage.UpdateIfValueIs(request.Id, request.Oldvalue, request.Newvalue);

                if(version.versionNumber < 0){
                     throw new RpcException(new Status(StatusCode.InvalidArgument, "Value to update did not match."));
                }

                return new DIDAVersion {
                VersionNumber = version.versionNumber,
                ReplicaId = version.replicaId,
            };
            }catch(DIDAStorageException e){
                throw new RpcException(new Status(StatusCode.InvalidArgument, e.ToString()));
            }
        }

        public override async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new LivenessCheckReply{Ok = true});
        }

        public override async Task<RemoveFailedStorageReply> removeFailedStorage(RemoveFailedStorageRequest request, ServerCallContext context)
        {
            if (_otherStorageNodes.ContainsKey(request.Id))
            {
                var keyValue = _otherStorageNodes[request.Id];
                _otherStorageNodes.TryRemove(request.Id, out keyValue);
                Console.WriteLine("Failed Storage with ID: {0} removed",request.Id);
            }

            return await Task.FromResult(new RemoveFailedStorageReply());
        }
    }
}