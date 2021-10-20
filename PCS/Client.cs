using System;
using System.Threading.Tasks;
using DIDAStorage.Proto;
using Grpc.Core;

namespace PCS
{
    public class Client
    {
        private readonly Channel _channel;
        
        private Client() {}

        public Client(string target)
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
        public bool WorkerLivenessCheck()
        {
            var client = new DIDAWorkerService.DIDAWorkerServiceClient(GetConnection());
            var response = client.livenessCheckAsync(new LivenessCheckRequest()).GetAwaiter().GetResult();
            ShutdownChannel();
            return response.Ok;
        }
        
        public DIDAListServerReply ListServerStorage()
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
            var response = client.listServer(new DIDAListServerRequest());
                
            ShutdownChannel();
            return response;
        }
        
        
        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}