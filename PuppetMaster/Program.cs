using System;
using System.Diagnostics;
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
            ShowTitle();

        while (!exit)
        {
            ShowMenu();

            string operation = Console.ReadLine();

            try
            {
                SelectOption(operation);
            }
            catch (Exception e)
            {
                Console.WriteLine("An exception occurred\n - Details: " + e.Message);
            }

            Console.WriteLine("------------------------\n");
            
            Console.Write("Press 'n' and Enter to close the app, or press any other key and Enter to continue: ");
            if (Console.ReadLine() == "n") exit = true;

            Console.WriteLine("\n");
            Console.Clear();
        }
        return;
        }

        private static void SelectOption(string operation)
        {
            
            switch (operation)
            {
                case "w":
                    ShowSubMenuWorker();
                    break;
                case "s":
                    ShowSubMenuStorage();
                    break;
                case "sch":
                    ShowSubMenuScheduler();
                    break;
                case "r":
                    break;
                // Return text for an incorrect option entry.
                default:
                    break;
            }
        }

        private static async void ShowSubMenuWorker()
        {
            Console.WriteLine("Create a new Worker as follows: server_id,url,gossip_delay and press enter:");
            Console.WriteLine("\texample: 123,localhost:10000,200");
            Console.WriteLine("\t--------------------------------------------------------------------");
            var parameter = Console.ReadLine();
            
            if (parameter != null)
            {
                var parameters = parameter.Split(",");
                if (parameters.Length == 3)
                {
                    var commandLine = new CommandLine();
                    Console.WriteLine(parameters);
                    var request = new PmCreateWorkerRequest {Id = int.Parse(parameters[0]), Url = parameters[1], GossipDelay = int.Parse(parameters[2])};
                    var result = await Task.FromResult(commandLine.createWorker(request));
                    Console.WriteLine(result.Result);
                }
            }
        }
        
        private static async void ShowSubMenuStorage()
        {
            Console.WriteLine("Create a new Storage as follows: server_id,url,gossip_delay and press enter:");
            Console.WriteLine("\texample: 123,localhost:10000,200");
            Console.WriteLine("\t---------------------------------------------------------------------");
            var parameter = Console.ReadLine();
            if (parameter != null)
            {
                var parameters = parameter.Split(",");
                if (parameters.Length == 3)
                {
                    var commandLine = new CommandLine();
                    Console.WriteLine(parameters);
                    var request = new PmCreateStorageRequest {Id = int.Parse(parameters[0]), Url = parameters[1], GossipDelay = int.Parse(parameters[2])};
                    var result = await Task.FromResult(commandLine.createStorage(request));
                    Console.WriteLine(result.Result);
                }
            }
        }
        
        private static async void ShowSubMenuScheduler()
        {
            Console.WriteLine("Create a new Scheduler as follows: server_id,url and press enter:");
            Console.WriteLine("\texample: 123,localhost:10000");
            Console.WriteLine("\t---------------------------------------------------------");
            var parameter = Console.ReadLine();
            if (parameter != null)
            {
                var parameters = parameter.Split(",");
                if (parameters.Length == 2)
                {
                    var commandLine = new CommandLine();
                    var request = new PmCreateSchedulerRequest() {Id = int.Parse(parameters[0]), Url = parameters[1]};
                    var result = await Task.FromResult(commandLine.createScheduler(request));
                    Console.WriteLine(result.Result);
                }
            }
        }

        private static void ShowMenu()
        {
            Console.WriteLine("Choose an operation from the following list:");
            Console.WriteLine("\tw - Create Worker");
            Console.WriteLine("\ts - Create Storage");
            Console.WriteLine("\tsch - Create Scheduler");
            Console.WriteLine("\tr - Run Application");
            Console.Write("Your option? ");
        }

        private static void ShowTitle()
        {
            Console.WriteLine("### Puppet Master App ###\r");
            Console.WriteLine("------------------------\n");
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
