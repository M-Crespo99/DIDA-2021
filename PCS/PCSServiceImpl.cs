using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PCSServiceImpl : PCSServiceBase
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;
        private static int Counter = 5000;
        
        private ConcurrentDictionary<int, string> _portWorker = new (ConcurrencyLevel, 100);
        private ConcurrentDictionary<int, string> _portStorage = new (ConcurrencyLevel, 100);
        public override async Task<PCSRunWorkerReply> runWorker(PCSRunWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            var counter = Interlocked.Increment(ref Counter);
            _portWorker.TryAdd(counter, String.Format("Worker-{0}", counter));

            return await Task.FromResult(new PCSRunWorkerReply {Ok = true});
        }

        public override async Task<PCSRunStorageReply> runStorage(PCSRunStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            
            var counter = Interlocked.Increment(ref Counter);
            _portStorage.TryAdd(counter, String.Format("Storage-{0}", counter));
            
            return await Task.FromResult(new PCSRunStorageReply {Ok = true});
        }
    }
}