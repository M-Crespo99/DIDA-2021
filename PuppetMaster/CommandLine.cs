using System.Threading.Tasks;

namespace PuppetMaster
{
    public class CommandLine
    {
        public async Task<PmCreateWorkerReply> createWorker(PmCreateWorkerRequest request)
        {
            var pcsClient = new PcsClient(request.Url);
            var response = pcsClient.CreateWorker(request.Id, request.Debug, request.GossipDelay);
            
            return await Task.FromResult(new PmCreateWorkerReply {Ok = response.Ok, Result = response.Result});
        }

        public async Task<PmCreateStorageReply> createStorage(PmCreateStorageRequest request)
        {
            var pcsClient = new PcsClient(request.Url);
            var response = pcsClient.CreateStorage(request.Id, false, request.GossipDelay);
            
            return await Task.FromResult(new PmCreateStorageReply {Ok = response.Ok, Result = response.Result});
        }

        public async Task<PmCreateSchedulerReply> createScheduler(PmCreateSchedulerRequest request)
        {
            var pcsClient = new PcsClient(request.Url);
            var response = pcsClient.CreateScheduler(request.Id, false);
            
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
            var pcsClient = new PcsClient("localhost:10000");
            var response = pcsClient.ListServer(request.Id);

            return await Task.FromResult(new PmListServerReply {Objects = {response.Objects}});
        }

        public async Task<PmRunApplicationReply> runApplication(PmRunApplicationRequest request)
        {
            var schedulerClient = new SchedulerClient(request.SchedulerUrl);
            var response = schedulerClient.SchedulerRunApplication(request);
            return await Task.FromResult(new PmRunApplicationReply {Ok = response.Ok});
        }
    }
}