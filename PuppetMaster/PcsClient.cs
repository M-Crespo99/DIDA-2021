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
                if (task.Status == TaskStatus.RanToCompletion) Console.WriteLine("");
            });

            return _channel;
        }

        public PCSRunWorkerReply CreateWorker(string id, bool debug, int gossipDelay, string url)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunWorkerRequest {Id = id, Debug = debug, GossipDelay = gossipDelay, Url = url};
            var response = client.runWorkerAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PCSRunStorageReply CreateStorage(string id, bool debug, int gossipDelay, string url)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunStorageRequest{Id = id, Debug = debug, GossipDelay = gossipDelay, Url = url};
            var response = client.runStorageAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PCSRunSchedulerReply CreateScheduler(string id, bool debug, string url)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PCSRunSchedulerRequest{Id = id, Debug = debug, Url = url};
            var response = client.runSchedulerAsync(request).GetAwaiter().GetResult();
            Console.WriteLine(response.Result);
            ShutdownChannel();
            return response;
        }
        
        public PcsListServerReply ListServer(string id)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var request = new PcsListServerRequest {Id = id};
            var response = client.listServerAsync(request).GetAwaiter().GetResult();
            ShutdownChannel();
            return response;
        }
        
        public PcsGetWorkersReply getWorkers()
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var response = client.getWorkersAsync(new PcsGetWorkersRequest()).GetAwaiter().GetResult();
            Console.WriteLine(response.Workers);
            ShutdownChannel();
            return response;
        }
        
        public PcsGetStoragesReply getStorages()
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var response = client.getStoragesAsync(new PcsGetStoragesRequest()).GetAwaiter().GetResult();
            Console.WriteLine(response.Storages);
            ShutdownChannel();
            return response;
        }
        
        public PcsGetSchedulerReply getScheduler()
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            var response = client.getSchedulerAsync(new PcsGetSchedulerRequest()).GetAwaiter().GetResult();
            Console.WriteLine(response.Scheduler);
            ShutdownChannel();
            return response;
        }
        
        public CrashReply CrashStorage(string storageId)
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            client.crashAsync(new CrashRequest {Id = storageId}).GetAwaiter().GetResult();
            ShutdownChannel();
            return new CrashReply{Ok = true};
        }
        
        public void PrintStatus()
        {
            var client = new PCSService.PCSServiceClient(GetConnection());
            client.statusAsync(new PcsStatusRequest()).GetAwaiter().GetResult();
            ShutdownChannel();
        }
        
        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}