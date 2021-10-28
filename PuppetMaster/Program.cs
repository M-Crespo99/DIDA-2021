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
        private const int Port = 2000;
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
            

        while (!exit)
        {
            ShowTitle();
            // ShowMenu();
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

            // Console.WriteLine("------------------------\n");
            //
            // Console.Write("Press 'n' and Enter to close the app, or press any other key and Enter to continue: ");
            // if (Console.ReadLine() == "n") exit = true;
            //
            // Console.WriteLine("\n");
            Console.Clear();
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
                    ShowSubMenuWorker(operation);
                    break;
                case "storage":
                    ShowSubMenuStorage(operation);
                    break;
                case "scheduler":
                    ShowSubMenuScheduler(operation);
                    break;
                case "client":
                    ShowSubMenuRunApplication(operation);
                    break;
                case "populate":
                    break;
                case "crash":
                    ShowSubMenuRCrashStorage(operation);
                    break;
                case "wait":
                    Wait(operation);
                    break;
                case "status":
                    Status(operation);
                    break;
                // Return text for an incorrect option entry.
                default:
                    break;
            }
        }

        private static void Status(string operation)
        {
            var result = operation.Split(" ");
            if (result.Length >= 1 && result[0].Trim().Equals("status"))
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
                catch (Exception e)
                {
                    Console.WriteLine("Argument of wait must be a number");
                }
            }
            else
            {
                Console.WriteLine("Command value is invalid, must be a valid number for wait");
            }
        }

        private static async void ShowSubMenuWorker(string command)
        {
            // Console.WriteLine("Create a new Worker as follows: server_id url gossip_delay and press enter:");
            // Console.WriteLine("\texample: 123 localhost:10000 200");
            // Console.WriteLine("\t--------------------------------------------------------------------");
            // var parameter = Console.ReadLine();
            
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 4)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateWorkerRequest {Id = parameters[1], Url = parameters[2], GossipDelay = int.Parse(parameters[3])};
                    var result = await Task.FromResult(commandLine.createWorker(request));
                    Console.WriteLine(result.Result);
                }
            }
        }
        
        private static async void ShowSubMenuStorage(string command)
        {
            // Console.WriteLine("Create a new Storage as follows: server_id,url,gossip_delay and press enter:");
            // Console.WriteLine("\texample: 123 localhost:10000 200");
            // Console.WriteLine("\t---------------------------------------------------------------------");
            // var parameter = Console.ReadLine();
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 4)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateStorageRequest {Id = parameters[1], Url = parameters[2], GossipDelay = int.Parse(parameters[3])};
                    var result = await Task.FromResult(commandLine.createStorage(request));
                    Console.WriteLine(result.Result);
                }
            }
        }
        
        private static async void ShowSubMenuScheduler(string command)
        {
            // Console.WriteLine("Create a new Scheduler as follows: server_id url and press enter:");
            // Console.WriteLine("\texample: 123 localhost:10000");
            // Console.WriteLine("\t---------------------------------------------------------");
            // var parameter = Console.ReadLine();
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 3)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateSchedulerRequest {Id = parameters[1], Url = parameters[2]};
                    var result = await Task
                        .FromResult(commandLine.createScheduler(request)).GetAwaiter().GetResult();
                    Console.WriteLine(result.Result);
                }
            }
        }
        
        private static async void ShowSubMenuRunApplication(string command)
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
                }
            }
        }
        
        private static async void ShowSubMenuRCrashStorage(string command)
        {
            if (command != null)
            {
                var parameters = command.Split(" ");
                if (parameters.Length == 2)
                {
                    var commandLine = new CommandLine();

                    var result = await Task.FromResult(commandLine.CrashStorage(parameters[1])).GetAwaiter().GetResult();
                    Console.WriteLine(result.Ok);
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
