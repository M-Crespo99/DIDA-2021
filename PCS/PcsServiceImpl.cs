using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PcsServiceImpl : PCSServiceBase
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;
        private static int _counter = 5000;
        
        private readonly ConcurrentDictionary<int, string> _portWorker = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _portStorage = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _portScheduler = new (ConcurrencyLevel, 100);
        public override async Task<PCSRunWorkerReply> runWorker(PCSRunWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                var counter = Interlocked.Increment(ref _counter);
                
                var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "worker")
                    .Replace("PCS", "worker");

                var argument = String.Format("{0}/bin/Debug/net5.0/worker.dll {1}", dir, counter);

                executeRunCommand("dotnet", argument);
                _portWorker.TryAdd(counter, String.Format("Worker-{0}", counter));
                return await Task.FromResult(new PCSRunWorkerReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunWorkerReply {Ok = false});
            }
        }

        private void executeRunCommand(string fileName, string argument)
        {
            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                ProcessStartInfo startInfo = new ProcessStartInfo { FileName = fileName, Arguments = argument}; 
                Process proc = new Process { StartInfo = startInfo, };
                proc.Start();
            }
        }

        public override async Task<PCSRunStorageReply> runStorage(PCSRunStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                var counter = Interlocked.Increment(ref _counter);
                
                var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "storage")
                    .Replace("PCS", "storage");

                var host = String.Format("localhost:{0}", counter);

                var argument = String.Format("{0}/bin/Debug/net5.0/storage.dll {1} {2} {3}", dir, request.Id, host , request.GossipDelay);
                executeRunCommand("dotnet", argument);
                _portStorage.TryAdd(counter, String.Format("Storage-{0}", counter));
                
                return await Task.FromResult(new PCSRunStorageReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunStorageReply {Ok = false});
            }
        }

        public override async Task<PCSRunSchedulerReply> runScheduler(PCSRunSchedulerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Scheduler ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                var counter = Interlocked.Increment(ref _counter);

               var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "scheduler")
                    .Replace("PCS", "scheduler");
                    
                var argument = String.Format("{0}/bin/Debug/net5.0/scheduler.dll {1}", dir, counter);
                executeRunCommand("dotnet", argument);
                _portScheduler.TryAdd(counter, String.Format("Scheduler-{0}", counter));
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = false});
            }
        }

        public override async Task<PmListServerReply> listServer(PmListServerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for List server ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            var response = new PmListServerReply();
            //TODO change to thread list
            var objects = new List<string>();
            foreach (var portWorkerKey in _portWorker.Keys)
            {
                var client = new Client(String.Format("localhost:{0}", portWorkerKey));
                var result = String.Format("The worker {0} liveness status is: {1}", _portWorker[portWorkerKey],
                    client.WorkerLivenessCheck());
                Console.WriteLine(result);
                objects.Add(result);
                
            }
            
            return await Task.FromResult(new PmListServerReply {Objects = { objects }});
        }
    }
}