using System;

namespace StorageTester
{
    class Program
    {
        static void Main(string[] args)

        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            StorageFrontend.StorageFrontend frontend = new StorageFrontend.StorageFrontend("localhost", 5001, true);
            Console.Write("> ");
            string input;
            while((input = Console.ReadLine()) != "quit"){
                string[] parts = input.Split(" ");
                if(parts.Length == 0){
                    Console.Write("> ");
                    continue;
                }
                if(parts[0] == "read"){
                    if(parts.Length == 2){
                        frontend.Read(parts[1]);
                    }
                    else if(parts.Length == 4){
                        DIDAStorage.Proto.DIDAVersion rv = new DIDAStorage.Proto.DIDAVersion { ReplicaId = Int32.Parse(parts[2]), VersionNumber = Int32.Parse(parts[3])};
                        frontend.Read(parts[1], rv);
                    }
                }
                else if(parts[0] == "write" && parts.Length == 3){
                    frontend.Write(parts[1], parts[2]);
                }
                else if(parts[0] == "update" && parts.Length == 4){
                    frontend.UpdateIfValueIs(parts[1], parts[2], parts[3]);
                }
                else if(parts[0] == "crash" && parts.Length == 1){
                    frontend.crashServer();
                }
                Console.Write("> ");
            }
        }
    }
}
