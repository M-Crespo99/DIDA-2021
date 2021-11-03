using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Reflection;
using Grpc.Reflection.V1Alpha;

namespace PuppetMaster
{
    class Program
    {
        private const int Port = 10001;
        static void Main(string[] args)
        {
            if (args.Length != 0 && args[0] == "GRPC-SERVER")
            {
                StartPuppetMasterAsGrpcServer();
            }
            else
            {
                StartCommandLine();
            }
        }

        private static void StartCommandLine()
        { 
            bool exit = false;
            
            
            ShowTitle();

            while (!exit)
            {

                // ShowMenu();
                Console.Write("> ");
                string command = Console.ReadLine();
                try
                {
                    
                    if (command != null)
                    {
                        if (command.Split(" ").Length > 0)
                        {
                            SelectOption(command);   
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("An exception occurred\n - Details: " + e.Message);
                }
            }
            return;
        }
        private static void ShowTitle()
        {
            Console.WriteLine("### Puppet Master App ###\r");
            Console.WriteLine("------------------------\n");
        }
        private static void ShowMenu()
        {
            Console.WriteLine("Choose an operation from the following list:");
            Console.WriteLine("\tworker - Create Worker");
            Console.WriteLine("\tstorage - Create Storage");
            Console.WriteLine("\tscheduler - Create Scheduler");
            Console.WriteLine("\tr - Run Application");
            Console.WriteLine("\tpopulate - Populate");
            Console.Write("Your option? ");
        }

        private static void SelectOption(string operation)
        {
            
            switch (operation.Split(" ")[0])
            {
                case "worker":
                    Worker(operation);
                    break;
                case "storage":
                    Storage(operation);
                    break;
                case "scheduler":
                    Scheduler(operation);
                    break;
                case "client":
                    RunApplication(operation);
                    break;
                case "populate":
                    Populate(operation);
                    break;
                case "crash":
                    CrashStorage(operation);
                    break;
                case "wait":
                    Wait(operation);
                    break;
                case "status":
                    Status(operation);
                    break;
                case "listServer":
                    ListServer(operation);
                    break;
                case "listGlobal":
                    ListGlobal();
                    break;
                default:
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Unknown command: {0}.", operation);
                    Console.ResetColor();

                    break;
            }
        }

        private static void Populate(string operation)
        {
            var result = operation.Split(" ");
            if (result.Length == 2)
            {
                var commandLine = new CommandLine();
                commandLine.Populate(result[1]);
            }else{
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Could run populate command: Could not parse command: {0}.", operation);
                Console.ResetColor();
            }
        }

        private static void ListGlobal()
        {
            var commandLine = new CommandLine();
            commandLine.ListGlobal();
        }
        private static void ListServer(string operation)
        {
            var result = operation.Split(" ");
            if (result.Length == 2)
            {
                var commandLine = new CommandLine();
                commandLine.ListServer(new PmListServerRequest{Id = result[1]}).GetAwaiter().GetResult();
            }else{
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Could not run listServer: Could not parse command: {0}.", operation);
                Console.ResetColor();
            }
        }
        private static void Status(string operation)
        {
            var result = operation.Split(" ");
            if (result.Length == 1 && result[0].Trim().Equals("status"))
            {
                var commandLine = new CommandLine();
                commandLine.PrintStatus();
            }
        }
        private static void Wait(string operation)
        {
            var result = operation.Split(" ");
            if (result.Length > 1)
            {
                try
                {
                    Console.WriteLine("Waiting {0} milliseconds", int.Parse(result[1]));
                    Thread.Sleep(int.Parse(result[1]));
                }
                catch (Exception )
                {
                    Console.WriteLine("Argument of wait must be a number");
                }
            }
            else
            {
                Console.WriteLine("Command value is invalid, must be a valid number for wait");
            }
        }

        private static async void Worker(string command)
        {

            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 4)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateWorkerRequest {Id = parameters[1], Url = parameters[2], GossipDelay = int.Parse(parameters[3])};
                    var result = await Task.FromResult(commandLine.createWorker(request));

                    if(result.Result.Ok){
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("Created Worker succesfully at {0}.", parameters[2]);
                        Console.ResetColor();
                    }else{
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Something went wrong creating the Worker.");
                        Console.ResetColor();
                    }
                }else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Could not create Worker: Could not parse command: {0}. Expected 3 arguments, got {1}.", command, parameters.Length - 1);
                    Console.ResetColor();
                }
            }
        }
        
        private static async void Storage(string command)
        {
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 4)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateStorageRequest {Id = parameters[1], Url = parameters[2], GossipDelay = int.Parse(parameters[3])};
                    var result = await Task.FromResult(commandLine.createStorage(request));

                    if(result.Result.Ok){
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("Created Storage succesfully at {0}.", parameters[2]);
                        Console.ResetColor();
                    }else{
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Something went wrong creating the storage.");
                        Console.ResetColor();
                    }
                }else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Could not create Storage: Could not parse command: {0}. Expected 3 arguments, got {1}.", command, parameters.Length - 1);
                    Console.ResetColor();
                }
            }
        }
        
        private static async void Scheduler(string command)
        {
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 3)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateSchedulerRequest {Id = parameters[1], Url = parameters[2]};
                    var result = await Task
                        .FromResult(commandLine.createScheduler(request)).GetAwaiter().GetResult();
                    
                    if(result.Ok){
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("Created Scheduler succesfully at {0}.", parameters[2]);
                        Console.ResetColor();
                    }else{
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Something went wrong creating the storage.");
                        Console.ResetColor();
                    }
                    return;
                }else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Could not create Scheduler: Could not parse command: {0}. Expected 2 arguments, got {1}.", command, parameters.Length - 1);
                    Console.ResetColor();
                }
            }
        }
        
        private static async void RunApplication(string command)
        {
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 3)
                {
                    var commandLine = new CommandLine();

                    var result = await Task
                        .FromResult(commandLine.runApplication(parameters[1], parameters[2])).GetAwaiter().GetResult();
                    Console.WriteLine(result.Ok);
                }else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Could not run application: Could not parse command: {0}.", command);
                    Console.ResetColor();
                }
            }
        }
        
        private static async void CrashStorage(string command)
        {
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 2)
                {
                    var commandLine = new CommandLine();

                    var result = await Task.FromResult(commandLine.CrashStorage(parameters[1])).GetAwaiter().GetResult();
                    Console.WriteLine(result.Ok);
                }else{
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Could not crash storage: Could not parse command: {0}.", command);
                    Console.ResetColor();
                }
            }
        }

        private static void StartPuppetMasterAsGrpcServer()
        {
            Server server = null;

            try
            {
                var reflectionServiceImpl = new ReflectionServiceImpl(PuppetMasterService.Descriptor, ServerReflection.Descriptor);
                server = new Server
                {  
                    Services = { PuppetMasterService.BindService(new PuppetMasterServiceImpl()), ServerReflection.BindService(reflectionServiceImpl) },
                    Ports = {new ServerPort("localhost", Port, ServerCredentials.Insecure)}
                };
                server.Start();
                Console.WriteLine("The Puppet Master server is listening on the port: " + Port);
     
                PcsStart();
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
            finally
            {
                if (server != null)
                {
                    server.ShutdownAsync().Wait();
                }
            }
        }

        private static void PcsStart()
        {
            var argument = Environment.CurrentDirectory.
                Replace("PuppetMaster", "PCS");
            
            ProcessStartInfo startInfo = new ProcessStartInfo { FileName = "dotnet", Arguments = String.Format("{0}/bin/Debug/net5.0/PCS.dll", argument)}; 
            Process proc = new Process { StartInfo = startInfo, };
            proc.Start();
        }
    }
}
