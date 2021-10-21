using System;
using System.Threading.Tasks;
using Google.Protobuf.Collections;
using Grpc.Core;

namespace PuppetMaster
{
    public class SchedulerClient
    {
        private readonly Channel _channel;
        
        private SchedulerClient() {}

        public SchedulerClient(string target)
        {
            _channel = new Channel(target, ChannelCredentials.Insecure);
        }

        private Channel GetConnection()
        {
            _channel.ConnectAsync().ContinueWith(task =>
            {
                if (task.Status == TaskStatus.RanToCompletion) Console.WriteLine("Client connected");
            });

            return _channel;
        }

        public PmRunApplicationReply SchedulerRunApplication(PmRunApplicationRequest pmRunApplicationRequest)
        {
            var client = new DIDASchedulerService.DIDASchedulerServiceClient(GetConnection());
            var request = new DIDARunApplicationRequest
            {
                Input = pmRunApplicationRequest.Input,
                Storages = { pmRunApplicationRequest.Storages },
                Workers = { pmRunApplicationRequest.Workers },
                FilePath = pmRunApplicationRequest.FilePath
            };
            Console.WriteLine(request);
            var response = client.runApplicationAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Ok);
            ShutdownChannel();
            return new PmRunApplicationReply {Ok = response.Ok};
        }
        
        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}