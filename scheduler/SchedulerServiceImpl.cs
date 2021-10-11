using System;
using System.Threading.Tasks;
using Grpc.Core;
using static DIDASchedulerService;

namespace scheduler
{
    public class SchedulerServiceImpl : DIDASchedulerServiceBase
    {
        public override async Task<DIDARunApplicationReply> runApplication(DIDARunApplicationRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run application ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new DIDARunApplicationReply{Ok = true});
        }

        public override async Task<DIDAAssignmentReply> assignOperator(DIDAAssignmentRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for assign operator ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new DIDAAssignmentReply{Ok = true});
        }
    }
}