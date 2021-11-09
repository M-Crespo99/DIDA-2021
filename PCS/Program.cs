using System;
using System.Threading;
using Grpc.Core;
using Grpc.Reflection;
using Grpc.Reflection.V1Alpha;

namespace PCS
{
    class Program
    {
        private const int Port = 10000;
        static void Main(string[] args)
        {
            Server server = null;

            try
            {
                var reflectionServiceImpl = new ReflectionServiceImpl(PCSService.Descriptor, ServerReflection.Descriptor);
                server = new Server
                {  
                    Services = { PCSService.BindService(new PcsServiceImpl()), ServerReflection.BindService(reflectionServiceImpl) },
                    Ports = {new ServerPort("localhost", Port, ServerCredentials.Insecure)}
                };
                server.Start();
                Console.WriteLine("The PCS server is listening on the port: " + Port);
                Scheduler.IntervalInSeconds(1, () => PcsServiceImpl.CheckStorageLiveness());
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
    }
}
