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
        private static int _counter = 0;
        
        private readonly ConcurrentDictionary<int, string> _worker = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _storage = new (ConcurrencyLevel, 100);
        private readonly ConcurrentDictionary<int, string> _scheduler = new (ConcurrencyLevel, 100);
        public override async Task<PmCreateWorkerReply> createWorker(PmCreateWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Create Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            
            var counter = Interlocked.Increment(ref _counter);
            _worker.TryAdd(counter, request.Url);

            var pcsClient = new PcsClient(request.Url);
            pcsClient.CreateWorker(counter, false, 0);
            
            return await Task.FromResult(new PmCreateWorkerReply {Ok = true});
        }

        public override async Task<PmCreateStorageReply> createStorage(PmCreateStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Create Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            
            var counter = Interlocked.Increment(ref _counter);
            _storage.TryAdd(counter, request.Url);
            
            var pcsClient = new PcsClient(request.Url);
            pcsClient.CreateStorage(counter, false, 0);
            
            return await Task.FromResult(new PmCreateStorageReply {Ok = true});
        }

        public override async Task<PmCreateSchedulerReply> createScheduler(PmCreateSchedulerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Create Scheduler ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            
            var counter = Interlocked.Increment(ref _counter);
            _scheduler.TryAdd(counter, request.Url);
            
            var pcsClient = new PcsClient(request.Url);
            pcsClient.CreateScheduler(counter, false);
            
            return await Task.FromResult(new PmCreateSchedulerReply {Ok = true});
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
    }
}