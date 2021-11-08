using System;
using System.IO;

namespace StorageTester
{
    class Program
    {
        static void Main(string[] args)

        {
            if(args.Length != 1){
                Console.WriteLine("Please provide port");
                return;
            }
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            StorageFrontend.StorageFrontend frontend = new StorageFrontend.StorageFrontend("localhost", Int32.Parse(args[0]), 2, true);
            Console.Write("> ");
            string input = "";
            while((input = Console.ReadLine()) != "quit" && input != null){
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
                    var reply = frontend.Write(parts[1], parts[2]);
                }
                else if(parts[0] == "update" && parts.Length == 4){
                    frontend.UpdateIfValueIs(parts[1], parts[2], parts[3]);
                }
                else if(parts[0] == "crash" && parts.Length == 1){
                    frontend.crashServer();
                }
                else if(parts[0] == "list" && parts.Length == 1){
                    var reply = frontend.listServer();
                    Console.WriteLine(reply.ToString());
                }
                else if(parts[0] == "status" && parts.Length == 1){
                    var reply = frontend.printStatus();
                    Console.WriteLine(reply.ToString());
                }
                else if(parts[0] == "debug" && parts.Length == 1){
                    var reply = frontend.toogleDebug();
                    Console.WriteLine(reply.Result.DebugStatus);
                }
                else if(parts[0] == "populate" && parts.Length == 2){
                    var file = parts[1];

                    file = Environment.CurrentDirectory + "/populate_files/" + file;
    	            string[] lines;
                    if((lines = readLines(file)) != null){
                        foreach(string line in lines){
                            var popParts = line.Split(",");
                            if(popParts.Length == 2){
                                frontend.Write(popParts[0], popParts[1]);
                            }else{
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine("Error processing populate file at line: {0}. Skipping line...", line);
                                Console.ResetColor();
                            }
                        }
                    }
                }
                else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Unknown Command: {0}", input);
                    Console.ResetColor();
                }
                Console.Write("> ");
            }
        }



        static string[] readLines(string filePath){
            string[] lines;
            if(!File.Exists(filePath)){
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write("ERROR");
                
                Console.WriteLine(": Could not open application file at: {0}. File does not exist.", filePath);
                Console.ResetColor();
                return null;
            }
            
            lines = System.IO.File.ReadAllLines(filePath);

            return lines;
        }
    }
}


