using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace PuppetMaster
{
    public class CommandLine
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;
        private static int _counter;
        private static string _pcsUrl = "localhost:10000";
        
        private readonly ConcurrentDictionary<int, string> _worker = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _storage = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _scheduler = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _pcs = new (ConcurrencyLevel, 100);

        private void getWorkersByPcsUrl(string url)
        {
            
        }
        public async Task<PmCreateWorkerReply> createWorker(PmCreateWorkerRequest request)
        {
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateWorker(request.Id, request.Debug, request.GossipDelay, request.Url);
            
            return await Task.FromResult(new PmCreateWorkerReply {Ok = response.Ok, Result = response.Result});
        }

        public async Task<PmCreateStorageReply> createStorage(PmCreateStorageRequest request)
        {
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateStorage(request.Id, false, request.GossipDelay, request.Url);
            
            return await Task.FromResult(new PmCreateStorageReply {Ok = response.Ok, Result = response.Result});
        }

        public async Task<PmCreateSchedulerReply> createScheduler(PmCreateSchedulerRequest request)
        {
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateScheduler(request.Id, false, request.Url);
            
            return await Task.FromResult(new PmCreateSchedulerReply {Ok = response.Ok, Result = response.Result});
        }

        // public async Task<PmCheckStatusReply> checkStatus(PmCheckStatusRequest request, ServerCallContext context)
        // {
        //     Console.WriteLine("## Testing parameters for Check Status ##");
        //     Console.WriteLine(request.ToString());
        //     Console.WriteLine("## ------ ##");
        //     
        //     return await base.checkStatus(request, context);
        // }

        // public async Task<PmListGlobalReply> listGlobal(PmListGlobalRequest request, ServerCallContext context)
        // {
        //     Console.WriteLine("## Testing parameters for List Global ##");
        //     Console.WriteLine(request.ToString());
        //     Console.WriteLine("## ------ ##");
        //         
        //     return await base.listGlobal(request, context);
        // }

        public async Task<PmListServerReply> listServer(PmListServerRequest request)
        {
            //TODO should look into the PCS available and not hard coded below
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.ListServer(request.Id);

            return await Task.FromResult(new PmListServerReply {Objects = {response.Objects}});
        }

        public async Task<PmRunApplicationReply> runApplication(string input, string filePath)
        {
            var pcsClient = new PcsClient(_pcsUrl);
            var scheduler = pcsClient.getScheduler().Scheduler;
            var schedulerClient = new SchedulerClient(scheduler);
            var pcsClientWorker = new PcsClient(_pcsUrl);
            var workers = pcsClientWorker.getWorkers().Workers;
            var pcsClientStorage = new PcsClient(_pcsUrl);
            var storages = pcsClientStorage.getStorages().Storages;
            var request = new PmRunApplicationRequest
            {
                Input = input,
                FilePath = filePath,
                Workers = { workers },
                Storages = { storages },
                SchedulerUrl = scheduler
            };
            var response = schedulerClient.SchedulerRunApplication(request);
            return await Task.FromResult(new PmRunApplicationReply {Ok = response.Ok});
        }
    }
}