using System;
using Grpc.Core;

namespace storage
{
    class Program
    {
        private static string host;
        private static int port;
        private static int server_id = 0; 

        private static string _serverName = "";

        private static int gossip_delay = 0;
        static void ShowUsage(){
            Console.WriteLine("Usage: ./storage server_id URL gossip_delay");
        }
        static bool ValidateInputArguments(string[] args){

            if(args.Length != 4 ){
                return false;
            }

            try{
                server_id = Int32.Parse(args[0]);
            }catch(FormatException){
                Console.WriteLine($"Unable to parse server id: '{args[0]}'");
                return false;
            }



            string[] URLParts = args[1].Replace("http://", "").Replace("https://", "").Split(":");

            if(URLParts.Length != 2){
                Console.WriteLine($"Unable to parse URL: '{args[1].Replace("http://", "").Replace("https://", "")}'");
                return false;
            }

            host = URLParts[0];

            try{
                port = Int32.Parse(URLParts[1]);

            }catch(FormatException){
                Console.WriteLine($"Unable to parse server port: '{args[1]}'");
                return false;
            }


            try{
                gossip_delay = Int32.Parse(args[2]);
            }catch(FormatException){
                Console.WriteLine($"Unable to parse gossip delay: '{args[2]}'");
                return false;
            }

            _serverName = args[3];

            return true;
        }

        static int Main(string[] args)
        {
            if(!ValidateInputArguments(args)){
                ShowUsage();
                return 1;
            }   
            Server server = new Server{
                Services = {DIDAStorage.Proto.DIDAStorageService.BindService(new StorageServerService(server_id, host, port, gossip_delay, _serverName))},
                Ports = { new ServerPort(host, port, ServerCredentials.Insecure) }
            };

            server.Start();
            Console.WriteLine("Storage Server Started.");
            Console.WriteLine("Server Name: " + _serverName);
            Console.WriteLine("Server ID: " + server_id);

            Console.WriteLine("Gossip Delay: " + gossip_delay);

            Console.WriteLine("Listening on: " + args[1]);
            Console.ReadKey();
            server.ShutdownAsync().Wait();
            return 0;
        }
    }
}
