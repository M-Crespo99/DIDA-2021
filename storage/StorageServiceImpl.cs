using System.Threading.Tasks;
using Grpc.Core;
using System;

namespace storage{
    public class StorageServerService : DIDAStorageService.DIDAStorageServiceBase{
        
        DIDAStorage.DIDAStorage storage;

        public StorageServerService(int replicaId){
            storage = new DIDAStorage.DIDAStorage(replicaId);
        }


        public override Task<DIDARecordReply> read(DIDAReadRequest request, ServerCallContext context){
            return Task.FromResult(processReadRequest(request));
        }

        public override Task<DIDAVersion> write(DIDAWriteRequest request, ServerCallContext context){
            return Task.FromResult(processWriteRequest(request));
        }


        private DIDARecordReply processReadRequest(DIDAReadRequest request){
            try{
                DIDAStorage.DIDAVersion version = new DIDAStorage.DIDAVersion();
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

                DIDARecordReply reply = new DIDARecordReply{
                    Id = request.Id,
                    Version = new DIDAVersion{
                        VersionNumber = record.version.versionNumber,
                        ReplicaId = record.version.replicaId
                    },
                    Val = record.val
                };
                return reply;

            }catch(DIDAStorage.Exceptions.DIDAStorageException e){
                Console.WriteLine("Exceptiomn caught");
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
    }
}