using System;
using Grpc.Net.Client;

namespace StorageTester
{
    class Program
    {
        static void Main(string[] args)

        {
            var channel = GrpcChannel.ForAddress("https://localhost:5001");
            DIDAStorageService.DIDAStorageServiceClient client = new DIDAStorageService.DIDAStorageServiceClient(channel);

            DIDAVersion v = client.write(new DIDAWriteRequest{
                Id = "21",
                Val = "Miguel"

            });
            
            Console.WriteLine("Version Number: " + v.VersionNumber);
            Console.WriteLine("Replica ID: " + v.ReplicaId);
            Console.WriteLine("Hello World!");
        }
    }
}
