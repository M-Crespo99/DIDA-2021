using System;
using System.Diagnostics;
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
