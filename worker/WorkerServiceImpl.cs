using System.Threading.Tasks;
using Grpc.Core;
using static DIDAWorkerService;

namespace worker
{
    public class WorkerServiceImpl : DIDAWorkerServiceBase
    {
        public override async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            return await Task.FromResult(new LivenessCheckReply{Ok = true});
        }
    }
}