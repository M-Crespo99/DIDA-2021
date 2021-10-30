using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PcsServiceImpl : PCSServiceBase
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;

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
                var argument = "";
                if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
                {
                    argument = String.Format("{0}/bin/Debug/net5.0/worker.dll {1} {2}", dir, newPort, request.Id);    
                }
                else
                {
                    argument = String.Format("{0}\\bin\\Debug\\net5.0\\worker.dll {1} {2}", dir, newPort, request.Id);
                }
                
                

                executeRunCommand("dotnet", argument);
                _idWorker.TryAdd(request.Id, request.Url);

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

                var argument = "";
                if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
                {
                    argument = String.Format("{0}/bin/Debug/net5.0/storage.dll {1} {2} {3}", dir, request.Id, request.Url , request.GossipDelay);    
                }
                else
                {
                    argument = String.Format("{0}\\bin\\Debug\\net5.0\\storage.dll {1} {2} {3}", dir, request.Id, request.Url , request.GossipDelay);
                }
                
                
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
               
                var newUrlAndPort = request.Url
                    .Replace("http://", "")
                    .Replace("https://", "")
                    .Split(":");
                var newHost = newUrlAndPort[0];
                var newPort = newUrlAndPort[1];
                
                var argument = "";
                if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
                {
                    argument = String.Format("{0}/bin/Debug/net5.0/scheduler.dll {1}", dir, newHost, newPort);    
                }
                else
                {
                    argument = String.Format("{0}\\bin\\Debug\\net5.0\\scheduler.dll {1}", dir, newHost, newPort);
                }
                
                executeRunCommand("dotnet", argument);
                _idScheduler.TryAdd(request.Id, request.Url);
                _schedulers[0] = request.Url;
                
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunSchedulerReply {Ok = false});
            }
        }

        // Lists all objects stored on the system.
        public override async Task<PcsListGlobalReply> listGlobal(PcsListGlobalRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Listing all objects stored on the system ##");
            foreach (var keyValuePair in _idHostStorage)
            {
                var client = new Client(keyValuePair.Value);
                client.ListServerStorage();
            }
            
            foreach (var keyValuePair in _idWorker)
            {
                var client = new Client(keyValuePair.Value);
                client.ListServerWorker();
            }

            return await Task.FromResult(new PcsListGlobalReply());
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

        public override async Task<PcsListServerReply> listServer(PcsListServerRequest request, ServerCallContext context)
        {
            var storageUrl = _idHostStorage[request.Id];
            if (storageUrl != null)
            {
                var client = new Client(storageUrl);
                var result = client.ListServerStorage();
            
                // foreach (var didaCompleteRecord in result.Records)
                // {
                //     Console.WriteLine(didaCompleteRecord.Id);
                //     foreach (var didaRecordReply in didaCompleteRecord.Versions)
                //     {
                //         Console.WriteLine(didaRecordReply.Id);   
                //         Console.WriteLine(didaRecordReply.Val);   
                //         Console.WriteLine(didaRecordReply.Version.ReplicaId);   
                //         Console.WriteLine(didaRecordReply.Version.VersionNumber);   
                //     }
                // }
            }
            var workerUrl = _idWorker[request.Id];
            if (workerUrl != null)
            {
                var clientWorker = new Client(workerUrl);
                var resultWorker = clientWorker.ListServerWorker();
                foreach (var resultWorkerDetail in resultWorker.Details)
                {
                    Console.WriteLine(resultWorkerDetail.OperatorName);
                    Console.WriteLine(resultWorkerDetail.TotalTime);
                    Console.WriteLine(resultWorkerDetail.Executions);
                }
            
                return await Task.FromResult(new PcsListServerReply());
            }
            
            return await Task.FromResult(new PcsListServerReply());
        }

        public override async Task<CrashReply> crash(CrashRequest request, ServerCallContext context)
        {
            var storageUrl = _idHostStorage[request.Id];
            if (storageUrl == null) return await Task.FromResult(new CrashReply {Ok = false});
            
            var client = new Client(storageUrl);
            client.CrashStorage();

            if (_idHostStorage.TryRemove(request.Id, out storageUrl))
            {
                Console.WriteLine("storage {0} crashed", storageUrl);    
            }
            return await Task.FromResult(new CrashReply {Ok = true});
        }

        public override async Task<DebugReply> debug(DebugRequest request, ServerCallContext context)
        {
            return await base.debug(request, context);
        }

        public override async Task<PcsStatusReply> status(PcsStatusRequest request, ServerCallContext context)
        {
            
            //TODO do the liveness first (if its up or not) for all below
            foreach (var keyValuePair in _idHostStorage)
            {
                var client = new Client(keyValuePair.Value);
                client.PrintStorageStatus();
            }

            foreach (var keyValuePair in _idWorker)
            {
                var client = new Client(keyValuePair.Value);
                client.PrintWorkerStatus();
            }
            
            // foreach (var scheduler in _schedulers)
            // {
            //     var client = new Client(scheduler);
            //     client.PrintSchedulerStatus();
            // }
            return await Task.FromResult(new PcsStatusReply());
        }

        public override async Task<PopulateReply> populate(PopulateRequest request, ServerCallContext context)
        {
            if (!_idHostStorage.IsEmpty)
            {
                try
                {
                    string[] lines = File.ReadAllLines(String.Format(@"{0}", request.DataFilePath));
                    var firstStorageUrl = _idHostStorage.Values.First();
                    var client = new Client(firstStorageUrl);
                    
                    if (lines.Length > 0) { client.WriteIntoStorage(lines); }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
            return await Task.FromResult(new PopulateReply());
        }
    }
}