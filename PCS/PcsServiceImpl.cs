using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PcsServiceImpl : PCSServiceBase
    {

        private int storageCount = 0;
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;

        private static readonly ConcurrentDictionary<string, string> IdWorker = new (ConcurrencyLevel, 100);
        private static readonly ConcurrentDictionary<string, string> IdHostStorage = new (ConcurrencyLevel, 100);
        private static readonly ConcurrentDictionary<string, string> IdScheduler = new (ConcurrencyLevel, 100);
        private readonly string [] _schedulers = new string[10];
        
        public static void CheckStorageLiveness()
        {
            ConcurrentDictionary<string, string> idHostStorageToRemove = new (ConcurrencyLevel, 100);
            foreach (var keyValuePair in IdHostStorage)
            {
                try
                {
                    var client = new Client(keyValuePair.Value);
                    if (!client.liveness())
                    {
                        Thread.Sleep(1000);
                        if (!client.liveness())
                        {
                            idHostStorageToRemove.TryAdd(keyValuePair.Key, keyValuePair.Value);    
                        }
                    }

                }catch(Exception e){
                    Console.WriteLine(e.ToString());
                }
                
            }
            
            foreach (var keyValuePair in idHostStorageToRemove)
            {
                var id = keyValuePair.Key;
                var host = keyValuePair.Value;
                if (IdHostStorage.TryRemove(id, out host))
                {
                    Console.WriteLine("Storage ID: {0} - Host: {1} failed", id, host);
                }
            }
            
            foreach (var idHostStorage in IdHostStorage)
            {
                foreach (var valuePair in idHostStorageToRemove)
                {
                    var client = new Client(idHostStorage.Value);
                    if (client.liveness())
                    {
                        client = new Client(idHostStorage.Value);
                        client.removeFailedStorage(valuePair.Key);
                    }
                }
                
            }
        }
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
                    argument = String.Format("{0}/bin/Debug/net5.0/worker.dll {1} {2} {3}", dir, newPort, request.Id, request.GossipDelay);    
                }
                else
                {
                    argument = String.Format("{0}\\bin\\Debug\\net5.0\\worker.dll {1} {2} {3}", dir, newPort, request.Id, request.GossipDelay);
                }
                
                

                executeRunCommand("dotnet", argument);
                IdWorker.TryAdd(request.Id, request.Url);

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

                var host = request.Url
                    .Replace("http://", "")
                    .Replace("https://", "")
                    .Split(":")[0];
                

                var argument = "";

                lock(this){
                    storageCount++;
                    if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
                    {
                        argument = String.Format("{0}/bin/Debug/net5.0/storage.dll {1} {2} {3} {4}", dir, storageCount, request.Url , request.GossipDelay, request.Id);    
                    }
                    else
                    {
                        argument = String.Format("{0}\\bin\\Debug\\net5.0\\storage.dll {1} {2} {3} {4}", dir, storageCount, request.Url , request.GossipDelay, request.Id);
                    }
                
                    executeRunCommand("dotnet", argument);
                }
                //Make sure all the storages know of the new storage
                foreach(var entry in IdHostStorage){
                    var client = new Client(entry.Value);
                    client.addStorage(request.Id, host, Int32.Parse(newPort));
                }

                IdHostStorage.TryAdd(request.Id, request.Url);

                //Make sure the new storage knows about all the other storages

                Console.WriteLine("Waiting for storage to initialize...");
                Thread.Sleep(750);
                Console.WriteLine("Done.");
                var clientForNewStorage = new Client(request.Url);
                foreach(var entry in IdHostStorage){
                    Console.WriteLine(entry.Value);
                    var url = entry.Value.Replace("http://", "").Replace("https://", "");
                    string entryHost = url.Split(":")[0];
                    int entryPort = Int32.Parse(url.Split(":")[1]);

                    //Dont send you to yourself
                    if((entryHost == host) && (entryPort == Int32.Parse(newPort))){
                        continue;
                    }
                    clientForNewStorage.addStorage(entry.Key, entryHost, entryPort);
                }

                return await Task.FromResult(new PCSRunStorageReply {Ok = true});
            }
            catch (Exception e)
            {
                lock(this){
                    storageCount--;
                }
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
                    argument = String.Format("{0}/bin/Debug/net5.0/scheduler.dll {1} {2}", dir, newHost, newPort);    
                }
                else
                {
                    argument = String.Format("{0}\\bin\\Debug\\net5.0\\scheduler.dll {1} {2}", dir, newHost, newPort);
                }
                
                executeRunCommand("dotnet", argument);
                IdScheduler.TryAdd(request.Id, request.Url);
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
            foreach (var keyValuePair in IdHostStorage)
            {
                var client = new Client(keyValuePair.Value);
                if (client.liveness())
                {
                    client = new Client(keyValuePair.Value);
                    client.ListServerStorage();    
                }
            }
            
            foreach (var keyValuePair in IdWorker)
            {
                var client = new Client(keyValuePair.Value);
                client.ListServerWorker();
            }

            return await Task.FromResult(new PcsListGlobalReply());
        }

        public override async Task<PcsGetStoragesReply> getStorages(PcsGetStoragesRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new PcsGetStoragesReply {Storages = { IdHostStorage.Values }});
        }

        public override async Task<PcsGetWorkersReply> getWorkers(PcsGetWorkersRequest request, ServerCallContext context)
        {
            var workers = IdWorker.Select(pair => pair.Key + "+" + pair.Value).ToArray();
            return await Task.FromResult(new PcsGetWorkersReply {Workers = { workers }});
        }

        public override async Task<PcsGetSchedulerReply> getScheduler(PcsGetSchedulerRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new PcsGetSchedulerReply {Scheduler = _schedulers[0]});
        }

        public override async Task<PcsListServerReply> listServer(PcsListServerRequest request, ServerCallContext context)
        {
            if (IdHostStorage.ContainsKey(request.Id))
            {
                var storageUrl = IdHostStorage[request.Id];
                var client = new Client(storageUrl);
                if (client.liveness())
                {
                    client = new Client(storageUrl);
                    var result = client.ListServerStorage();    
                }
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
            
            if (IdWorker.ContainsKey(request.Id))
            {
                var workerUrl = IdWorker[request.Id];
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
            var storageUrl = IdHostStorage[request.Id];
            if (storageUrl == null) return await Task.FromResult(new CrashReply {Ok = false});
            
            var client = new Client(storageUrl);
            client.CrashStorage();

            if (IdHostStorage.TryRemove(request.Id, out storageUrl))
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
            
            foreach (var keyValuePair in IdHostStorage)
            {
                var client = new Client(keyValuePair.Value);
                if (client.liveness())
                {
                    client = new Client(keyValuePair.Value);
                    client.PrintStorageStatus();    
                }
            }

            foreach (var keyValuePair in IdWorker)
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
            if (!IdHostStorage.IsEmpty)
            {
                try
                {
                    var dir = Environment.CurrentDirectory;
                    string[] lines = File.ReadAllLines(String.Format(dir + "/populate_files/" + request.DataFilePath));
                    var firstStorageUrl = IdHostStorage.Values.First();



                    var client = new Client(firstStorageUrl);
                    
                    if (lines.Length > 0) { client.WriteIntoStorage(lines, IdHostStorage.Count); }
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