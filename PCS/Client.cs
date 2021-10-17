using System;
using System.Threading.Tasks;
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

        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }
    }
}