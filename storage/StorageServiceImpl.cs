using System.Threading.Tasks;
using Grpc.Core;
using System;

namespace storage{
    public class StorageServerService : DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceBase{
        
        DIDAStorage.Storage storage;

        public StorageServerService(int replicaId){
            storage = new DIDAStorage.Storage(replicaId, true);
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
            return Task.FromResult(new DIDAStorage.Proto.DIDACrashServerReply());
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

        //TODO: UpdateIfValueIs
    }
}