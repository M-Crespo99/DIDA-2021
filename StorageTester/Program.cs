using System;
using Grpc.Net.Client;
using Grpc.Core;

namespace StorageTester
{
    class Program
    {
        static void Main(string[] args)

        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            var channel = GrpcChannel.ForAddress("http://localhost:5001");
            DIDAStorageService.DIDAStorageServiceClient client = new DIDAStorageService.DIDAStorageServiceClient(channel);

            DIDAVersion v = client.write(new DIDAWriteRequest{
                Id = "Miguel",
                Val = "21"

            });
            Console.WriteLine("Version Number: " + v.VersionNumber);
            Console.WriteLine("Replica ID: " + v.ReplicaId);

            v = client.write(new DIDAWriteRequest{
                Id = "Joao",
                Val = "42"
            });
            Console.WriteLine("yeet");

            try{
                DIDARecordReply r = client.read(new DIDAReadRequest{
                Id = "Miguel",
                Version = new DIDAVersion{VersionNumber = 10,
                ReplicaId = 0}
                });
                Console.WriteLine("Version Number: " + v.VersionNumber);
                Console.WriteLine("Replica ID: " + v.ReplicaId);

                Console.WriteLine("READ ID: " + r.Id);
                Console.WriteLine("READ Replica ID: " + r.Version.ReplicaId);
                Console.WriteLine("READ Version Nyumvber: " + r.Version.VersionNumber);
                Console.WriteLine("READ VAlue : " + r.Val);
                Console.WriteLine("Hello World!");
            }catch(RpcException e){

                Console.WriteLine("Exception: " + e.Message);
            }
            
            
            
            
        }
    }
}
