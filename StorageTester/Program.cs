using System;
using Grpc.Net.Client;
using Grpc.Core;
using System.Collections.Generic;

namespace StorageTester
{
    class Program
    {
        static void Main(string[] args)

        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            var channel = GrpcChannel.ForAddress("http://localhost:5001");
            DIDAStorageService.DIDAStorageServiceClient client = new DIDAStorageService.DIDAStorageServiceClient(channel);

            Console.Write("> ");
            string input;         
            while((input = Console.ReadLine()) != "quit"){
                string[] parts = input.Split(" ");
                DIDARecordReply r = null;
                if(parts.Length == 0){
                    Console.Write("> ");
                    continue;
                }

                if(parts[0] == "read"){
                   try {
                    if(parts.Length == 2){
                        r = client.read(new DIDAReadRequest{
                        Id = parts[1],
                        Version = null
                        });
                    }
                    else if(parts.Length == 4){
                        DIDAVersion rv = new DIDAVersion { ReplicaId = Int32.Parse(parts[2]), VersionNumber = Int32.Parse(parts[3])};
                        r = client.read(new DIDAReadRequest{
                        Id = parts[1],
                        Version = rv
                        });
                    }
                    if(r != null){
                        Console.WriteLine("READ ID: " + r.Id);
                        Console.WriteLine("READ Replica ID: " + r.Version.ReplicaId);
                        Console.WriteLine("READ Version Number: " + r.Version.VersionNumber);
                        Console.WriteLine("READ VAlue : " + r.Val);
                    }
                    }
                    catch(RpcException e){
                        Console.WriteLine("ERROR: " + e.Message);
                    }   
                    
                }
                else if(parts[0] == "write" && parts.Length == 3){
                    try
                    {
                         DIDAVersion v = client.write(new DIDAWriteRequest{
                    Id = parts[1],
                    Val = parts[2]
                    });

                    Console.WriteLine("Version Number: " + v.VersionNumber);
                    Console.WriteLine("Replica ID: " + v.ReplicaId);
                    }
                    catch(RpcException e){
                        Console.WriteLine("ERROR: " + e.Message);
                    }   
                }else{
                    Console.WriteLine("ERROR: Can't Process: " + input);
                }

                Console.Write("> ");
            }
            
        }
    }
}
