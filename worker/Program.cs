﻿using System;
using Grpc.Core;
using Grpc.Reflection;
using Grpc.Reflection.V1Alpha;
using static Grpc.Core.ServerCredentials;

namespace worker
{
    class Program
    {
        static void Main(string[] args)
        {
            Server server = null;

            try
            {
                var port = int.Parse(args[0]);
                server = new Server
                {
                    Ports = {new ServerPort("localhost", port, Insecure)}
                };
                server.Start();
                Console.WriteLine("The Worker server is listening on the port: " + port);
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
