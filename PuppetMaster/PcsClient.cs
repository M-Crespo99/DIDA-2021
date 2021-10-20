using System;
using System.Threading.Tasks;
using Grpc.Core;

namespace PuppetMaster
{
    public class PcsClient
    {
        private readonly Channel _channel;
        
        private PcsClient() {}

        public PcsClient(string target)
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

        public PCSRunWorkerReply CreateWorker(int id, bool debug, int gossipDelay)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunWorkerRequest {Id = id.ToString(), Debug = debug, GossipDelay = gossipDelay};
            var response = client.runWorkerAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PCSRunStorageReply CreateStorage(int id, bool debug, int gossipDelay)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunStorageRequest{Id = id.ToString(), Debug = debug, GossipDelay = gossipDelay};
            var response = client.runStorageAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PCSRunSchedulerReply CreateScheduler(int id, bool debug)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunSchedulerRequest{Id = id.ToString(), Debug = debug};
            var response = client.runSchedulerAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PcsListServerReply ListServer(int id)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PcsListServerRequest {Id = id};
            var response = client.listServerAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Objects);
            ShutdownChannel();
            return response;
        }
        
        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}