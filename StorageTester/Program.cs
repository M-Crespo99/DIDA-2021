using System;
using Grpc.Net.Client;

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

            DIDARecordReply r = client.read(new DIDAReadRequest{
                Id = "Joao",
                Version = v
            });
            
            
            Console.WriteLine("Version Number: " + v.VersionNumber);
            Console.WriteLine("Replica ID: " + v.ReplicaId);

            Console.WriteLine("READ ID: " + r.Id);
            Console.WriteLine("READ Replica ID: " + r.Version.ReplicaId);
            Console.WriteLine("READ Version Nyumvber: " + r.Version.VersionNumber);
            Console.WriteLine("READ VAlue : " + r.Val);
            Console.WriteLine("Hello World!");
        }
    }
}
