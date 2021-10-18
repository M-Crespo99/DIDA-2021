using System;
using System.Collections.Generic;
using Grpc;
using Grpc.Core;
using Grpc.Net.Client;
using DIDAWorker.Proto;

namespace worker
{
    class Program
    {
        static void Main(string[] args)
        {
            Server worker1 = new Server
            {
                Services = { DIDAWorkerService.BindService(new WorkerImpl()) },
                Ports = { new ServerPort("localhost", 20001, ServerCredentials.Insecure) }
            };

            Server worker2 = new Server
            {
                Services = { DIDAWorkerService.BindService(new WorkerImpl()) },
                Ports = { new ServerPort("localhost", 20002, ServerCredentials.Insecure) }
            };
            
            worker1.Start();
            worker2.Start();

            var channel = GrpcChannel.ForAddress("http://localhost:20001");
            var client = new DIDAWorkerService.DIDAWorkerServiceClient(channel);

            DIDARequest request = new DIDARequest
            {
                Meta = new DIDAMetaRecord {Id = 1},
                Input = "",
                Next = 0,
                ChainSize = 2
            };
            request.Chain.Add(new DIDAAssignment
            {
                Operator = new DIDAOperatorID
                {
                    Classname = "CounterOperator",
                    Order = 0
                },
                Host = "localhost",
                Port = 20001,
                Output = ""
            });
            
            request.Chain.Add(new DIDAAssignment
            {
                Operator = new DIDAOperatorID
                {
                    Classname = "CounterOperator",
                    Order = 1
                },
                Host = "localhost",
                Port = 20002,
                Output = ""
            });

            var reply = client.workOnOperator(request);
            System.Console.WriteLine("next");
        }
    }
}
