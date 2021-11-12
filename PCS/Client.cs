using System;
using DIDAStorage.Proto;
using DIDAWorker.Proto;
using Grpc.Core;
using System.Collections.Generic;
using StatusRequest = DIDAStorage.Proto.StatusRequest;

namespace PCS
{
    public class Client
    {
        private readonly Channel _channel;
        
        private Client() {}

        public Client(string target)
        {
            target = target.Replace("http://", "").Replace("https://", "");
            _channel = new Channel(target, ChannelCredentials.Insecure);
        }

        private Channel GetConnection()
        {
            _channel.ConnectAsync().ContinueWith(task =>
            {
                // if (task.Status == TaskStatus.RanToCompletion) Console.WriteLine("");
            });

            return _channel;
        }
        // public bool WorkerLivenessCheck()
        // {
        //     var client = new DIDAWorkerService.DIDAWorkerServiceClient(GetConnection());
        //     var response = client.livenessCheckAsync(new LivenessCheckRequest()).GetAwaiter().GetResult();
        //     ShutdownChannel();
        //     return response.Ok;
        // }
        
        public DIDAListServerReply ListServerStorage()
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
            var response = client.listServerAsync(new DIDAListServerRequest()).GetAwaiter().GetResult();
                
            ShutdownChannel();
            return response;
        }
        
        public bool liveness()
        {
            lock (this)
            {
                var res = false;
                try
                {
                    var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
                    var response = client.livenessCheckAsync(new LivenessCheckRequest()).GetAwaiter().GetResult();
                    res = response.Ok;
                    ShutdownChannel();
                }
                catch (Exception e) {
                    Console.WriteLine(e.ToString());
                }
                
                return res;
            }
        }
        
        public void removeFailedStorage(string id)
        {
            try
            {
                var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
                client.removeFailedStorageAsync(new RemoveFailedStorageRequest{Id = id}).GetAwaiter().GetResult();
                ShutdownChannel();
            }
            catch (Exception e) {
                Console.WriteLine(e.ToString());
            }
        }
        
        public ListServerReply ListServerWorker()
        {
            
            var client = new DIDAWorkerService.DIDAWorkerServiceClient(GetConnection());
            var response = client.listServerAsync(new ListServerRequest()).GetAwaiter().GetResult();
                
            ShutdownChannel();
            return response;
        }

        public void CrashStorage()
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
            var request = new  DIDACrashServerRequest();
            
            try
            {
                client.crashServerAsync(request).GetAwaiter().GetResult();
            }
            finally
            {
                ShutdownChannel();
            }
        }
        
        public void PrintStorageStatus()
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());
            client.statusAsync(new StatusRequest()).GetAwaiter().GetResult();
            ShutdownChannel();
        }
        
        public void PrintWorkerStatus()
        {
            var client = new DIDAWorkerService.DIDAWorkerServiceClient(GetConnection());
            client.statusAsync(new DIDAWorker.Proto.StatusRequest()).GetAwaiter().GetResult();
            ShutdownChannel();
        }

        public void PrintSchedulerStatus()
        {
            
        }

        private void ShutdownChannel()
        {
            _channel.ShutdownAsync().Wait();
        }

        public void WriteIntoStorage(string[] lines, int numberOfStorages)
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());

            var rand = new Random();
            foreach (var line in lines)
            {
                var idValue = line.Split(",");
                GossipLib.LamportClock lClock = new GossipLib.LamportClock(numberOfStorages);
                client.writeAsync(new DIDAWriteRequest {Id = idValue[0], Val = idValue[1], Clock = LClockToProto(lClock), UniqueID = rand.Next()}).GetAwaiter().GetResult();
            }
            ShutdownChannel();
        }

        public void addStorage(string id, string host, int port)
        {
            var client = new DIDAStorageService.DIDAStorageServiceClient(GetConnection());

            client.addStorage(new AddStorageRequest { Host = host, Port = port, Id = id});
            
        }
        private GossipLib.LamportClock protoToLClock(DIDAStorage.Proto.LamportClock protoClock){
            List<int> l = new List<int>();

            foreach(var value in protoClock.Values){
                l.Add(value);
            }

            return new GossipLib.LamportClock(l);
        }

        private DIDAStorage.Proto.LamportClock LClockToProto(GossipLib.LamportClock c){
            DIDAStorage.Proto.LamportClock protoLClock = new DIDAStorage.Proto.LamportClock();

            var l = c.toList();

            foreach(var value in l){
                protoLClock.Values.Add(value);
            }

            return protoLClock;
        }

    }
}