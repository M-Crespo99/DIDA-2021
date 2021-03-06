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
            _channel = new Channel(target.Replace("https://", "").Replace("http://", ""), ChannelCredentials.Insecure);
        }

        private Channel GetConnection()
        {
            _channel.ConnectAsync().ContinueWith(task =>
            {
                if (task.Status == TaskStatus.RanToCompletion) Console.WriteLine("");
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
            Console.WriteLine("Before client call");
            try
            {
                var response = client.runApplicationAsync(request).GetAwaiter().GetResult();
                Console.WriteLine("After client call");
                ShutdownChannel();
                return new PmRunApplicationReply {Ok = response.Ok};
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            return new PmRunApplicationReply {Ok = false};
        }
        
        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}