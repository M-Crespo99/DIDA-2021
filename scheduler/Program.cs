using System;
using Grpc.Core;
using Grpc.Reflection;
using Grpc.Reflection.V1Alpha;

namespace scheduler
{
    class Program
    {
        static void Main(string[] args)
        {
            Server server = null;

            try
            {
                var port = int.Parse(args[0]);
                var reflectionServiceImpl = new ReflectionServiceImpl(DIDASchedulerService.Descriptor, ServerReflection.Descriptor);
                server = new Server
                {  
                    Services = { DIDASchedulerService.BindService(new SchedulerServiceImpl()), ServerReflection.BindService(reflectionServiceImpl) },
                    Ports = {new ServerPort("localhost", port, ServerCredentials.Insecure)}
                };
                server.Start();
                Console.WriteLine("The Scheduler server is listening on the port: " + port);
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
