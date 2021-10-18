using System;
using System.Threading.Tasks;
using Grpc.Core;
using static DIDAWorkerService;

namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerServiceBase
    {
        public override async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing liveness check for worker##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new LivenessCheckReply{Ok = true});
        }
    }
}