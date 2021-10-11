using System;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PCSServiceImpl : PCSServiceBase
    {
        public override async Task<PCSRunWorkerReply> runWorker(PCSRunWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            return await Task.FromResult(new PCSRunWorkerReply {Ok = true});
        }

        public override async Task<PCSRunStorageReply> runStorage(PCSRunStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new PCSRunStorageReply {Ok = true});
        }
    }
}