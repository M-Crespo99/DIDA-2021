using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using static PuppetMasterService;

namespace PuppetMaster
{
    public class PuppetMasterServiceImpl : PuppetMasterServiceBase 
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;
        private static int _counter;
        private static string _pcsUrl = "localhost:10000";
        
        private readonly ConcurrentDictionary<string, string> _worker = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<string, string> _storage = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<string, string> _scheduler = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _pcs = new (ConcurrencyLevel, 100);
        public override async Task<PmCreateWorkerReply> createWorker(PmCreateWorkerRequest request, ServerCallContext context)
        {
            // var id = Interlocked.Increment(ref _counter);
            _worker.TryAdd(request.Id, request.Url);

            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateWorker(request.Id, request.Debug, request.GossipDelay, request.Url);
            
            return await Task.FromResult(new PmCreateWorkerReply {Ok = response.Ok, Result = response.Result});
        }

        public override async Task<PmCreateStorageReply> createStorage(PmCreateStorageRequest request, ServerCallContext context)
        {
            //TODO why not use the generated ID below
            var id = Interlocked.Increment(ref _counter);
            _storage.TryAdd(request.Id, request.Url);
            
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateStorage(request.Id, false, request.GossipDelay, request.Url);
            
            return await Task.FromResult(new PmCreateStorageReply {Ok = response.Ok, Result = response.Result});
        }

        public override async Task<PmCreateSchedulerReply> createScheduler(PmCreateSchedulerRequest request, ServerCallContext context)
        {
            // var counter = Interlocked.Increment(ref _counter);
            _scheduler.TryAdd(request.Id, request.Url);
            
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.CreateScheduler(request.Id, false, request.Url);
            
            return await Task.FromResult(new PmCreateSchedulerReply {Ok = response.Ok, Result = response.Result});
        }

        public override async Task<PmCheckStatusReply> checkStatus(PmCheckStatusRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Check Status ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            
            return await base.checkStatus(request, context);
        }

        public override async Task<PmListGlobalReply> listGlobal(PmListGlobalRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for List Global ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
                
            return await base.listGlobal(request, context);
        }

        public override async Task<PmListServerReply> listServer(PmListServerRequest request, ServerCallContext context)
        {
            //TODO should look into the PCS available and not hard coded below
            var pcsClient = new PcsClient(_pcsUrl);
            var response = pcsClient.ListServer(request.Id);

            return await Task.FromResult(new PmListServerReply ());
        }

        public override async Task<PmRunApplicationReply> runApplication(PmRunApplicationRequest request, ServerCallContext context)
        {
            var schedulerClient = new SchedulerClient(request.SchedulerUrl);
            var response = schedulerClient.SchedulerRunApplication(request);
            return await Task.FromResult(new PmRunApplicationReply {Ok = response.Ok});
        }

        public override Task<DebugInfoReply> receiveDebugInfo(DebugInfoRequest request, ServerCallContext context)
        {
            Console.WriteLine(request.Info);
            return Task.FromResult(new DebugInfoReply());
        }
    }
}