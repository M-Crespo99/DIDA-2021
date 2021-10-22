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
        private static int _port = 5000;
        
        private readonly ConcurrentDictionary<string, string> _idWorker = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<string, string> _idHostStorage = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<string, string> _idScheduler = new (ConcurrencyLevel, 100);
        private readonly string [] _schedulers = new string[10];
        public override async Task<PCSRunWorkerReply> runWorker(PCSRunWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Creating Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                // var newPort = Interlocked.Increment(ref _port);
                
                var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "worker")
                    .Replace("PCS", "worker");
                
                var newPort = request.Url
                    .Replace("http://", "")
                    .Replace("https://", "")
                    .Split(":")[1];

                var argument = String.Format("{0}/bin/Debug/net5.0/worker.dll {1}", dir, newPort);

                executeRunCommand("dotnet", argument);
                _idWorker.TryAdd(request.Id, String.Format("localhost:{0}", newPort));
                
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
            ProcessStartInfo startInfo = new ProcessStartInfo { FileName = fileName, Arguments = argument}; 
            Process proc = new Process { StartInfo = startInfo, };
            proc.Start();
        }

        public override async Task<PCSRunStorageReply> runStorage(PCSRunStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("##Creating Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                // var newPort = Interlocked.Increment(ref _port);
                
                var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "storage")
                    .Replace("PCS", "storage");
                
                var newPort = request.Url
                    .Replace("http://", "")
                    .Replace("https://", "")
                    .Split(":")[1];

                var host = String.Format("localhost:{0}", newPort);

                var argument = String.Format("{0}/bin/Debug/net5.0/storage.dll {1} {2} {3}", dir, request.Id, request.Url , request.GossipDelay);
                executeRunCommand("dotnet", argument);
                _idHostStorage.TryAdd(request.Id, request.Url);

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
            Console.WriteLine("## Creating Scheduler ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                // var newPort = Interlocked.Increment(ref _port);

               var dir = Environment.CurrentDirectory
                    .Replace("PuppetMaster", "scheduler")
                    .Replace("PCS", "scheduler");
               
                var newPort = request.Url
                    .Replace("http://", "")
                    .Replace("https://", "")
                    .Split(":")[1];
                
                var argument = String.Format("{0}/bin/Debug/net5.0/scheduler.dll {1}", dir, newPort);
                executeRunCommand("dotnet", argument);
                _idScheduler.TryAdd(request.Id, String.Format("localhost:{0}", newPort));
                _schedulers[0] = request.Url;
                
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = false});
            }
        }

        //Lists all objects stored on the server identified by server id
        // public override async Task<PcsListServerReply> listServer(PcsListServerRequest request, ServerCallContext context)
        // {
        //     Console.WriteLine("## Listing server with the ID: ##"+request.Id);
        //     Console.WriteLine(request.ToString());
        //     Console.WriteLine("## ------ ##");
        //     List<string> objects = new List<string>();
        //     
        //     if (_idHostStorage[request.Id] != null)
        //     {
        //         var client = new Client(_idHostStorage[request.Id]);
        //         var result = client.ListServerStorage();
        //         foreach (var didaCompleteRecord in result.Records)
        //         {
        //             objects.Add(didaCompleteRecord.ToString());
        //         }
        //         Console.WriteLine(objects);
        //         return await Task.FromResult(new PcsListServerReply {Objects = { objects }});
        //     }
        //     return await Task.FromResult(new PcsListServerReply {Objects = { objects }});
        // }

        // Lists all objects stored on the system.
        public override async Task<PcsListGlobalReply> listGlobal(PcsListGlobalRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Listing all objects stored on the system ##");
            return await base.listGlobal(request, context);
        }

        public override async Task<PcsGetStoragesReply> getStorages(PcsGetStoragesRequest request, ServerCallContext context)
        {

            return await Task.FromResult(new PcsGetStoragesReply {Storages = { _idHostStorage.Values }});
        }

        public override async Task<PcsGetWorkersReply> getWorkers(PcsGetWorkersRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new PcsGetWorkersReply {Workers = { _idWorker.Values }});
        }

        public override async Task<PcsGetSchedulerReply> getScheduler(PcsGetSchedulerRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new PcsGetSchedulerReply {Scheduler = _schedulers[0]});
        }
    }
}