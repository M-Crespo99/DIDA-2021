using Grpc.Core;
using Grpc.Net.Client;
using System.Threading.Tasks;
using System;

namespace StorageFrontend
{
    public class StorageFrontend
    {
        private int _port;

        private string _host;
        private GrpcChannel _channel;
        private DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient _client;

        private string _lastErrorMessage = "";

        private bool _verbose = false;

        public StorageFrontend(string host, int port)
        {
            this._port = port;
            this._host = host;

            this._verbose = false;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);
        }

        public StorageFrontend(string host, int port, bool verbose)
        {
            this._port = port;
            this._host = host;

            this._verbose = verbose;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);
        }

        public async Task<DIDAStorage.Proto.StatusReply> printStatus()
        {
            return await this._client.statusAsync(new DIDAStorage.Proto.StatusRequest());
        }

        public DIDAStorage.Proto.DIDAVersion Write(string id, string value)
        {
            DIDAStorage.Proto.DIDAWriteRequest writeRequest = new DIDAStorage.Proto.DIDAWriteRequest
            {
                Id = id,
                Val = value
            };
            try{
                var reply = this._client.write(writeRequest);

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("%% Write Operation complete on Storage node at " + this._host + ":" + this._port + " %%");
                    Console.WriteLine("WRITE ID: " + id);
                    Console.WriteLine("WRITE Value: " + value);
                    Console.WriteLine("New Version Number: " + reply.VersionNumber);
                    Console.WriteLine("Replica ID: " + reply.ReplicaId);
                    Console.ResetColor();
                }

                return reply;

            }catch(RpcException e){
                this._lastErrorMessage = e.Message;

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0}", e.Message);
                    Console.ResetColor();
                }
                
                return null;
            }
        }


        public async Task<DIDAStorage.Proto.ToggleDebugReply> toogleDebug()
        {
            return await this._client.toggleDebugAsync(new DIDAStorage.Proto.ToggleDebugRequest());
        }
        public DIDAStorage.Proto.DIDARecordReply Read(string id, int versionNumber, int replicaId)
        {
            var version = new DIDAStorage.Proto.DIDAVersion{
                    VersionNumber = versionNumber,
                    ReplicaId = replicaId
                };
            return Read(id, version);
        }

        public DIDAStorage.Proto.DIDARecordReply Read(string id, DIDAStorage.Proto.DIDAVersion version)
        {
            DIDAStorage.Proto.DIDAReadRequest readRequest = new DIDAStorage.Proto.DIDAReadRequest
            {
                Id = id,
                Version = version
            };
            try{
                var reply = this._client.read(readRequest);

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("%% Reading from Storage node at " + this._host + ":" + this._port + " %%");
                    Console.WriteLine("READ ID: " + reply.Id);
                    Console.WriteLine("READ Value : " + reply.Val);
                    Console.WriteLine("READ Replica ID: " + reply.Version.ReplicaId);
                    Console.WriteLine("READ Version Number: " + reply.Version.VersionNumber);
                    Console.ResetColor();
                }
                return reply;
            }
            catch(RpcException e){
                
                this._lastErrorMessage = e.Message;

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0}", e.Message);
                    Console.ResetColor();
                }

                return null;
            }
        }

        public DIDAStorage.Proto.DIDARecordReply Read(string id)
        {
            return this.Read(id, null);
        }

        public string getLastErrorMessage(){
            return this._lastErrorMessage;
        }

        public DIDAStorage.Proto.DIDAVersion UpdateIfValueIs(string id, string oldValue, string newValue){
            DIDAStorage.Proto.DIDAUpdateIfRequest request = new DIDAStorage.Proto.DIDAUpdateIfRequest(){
                Id = id, 
                Oldvalue = oldValue,
                Newvalue = newValue
            };

            try{
                var reply = this._client.updateIfValueIs(request);

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("%% Update Value Operation at Storage node " + this._host + ":" + this._port + " %%");
                    Console.WriteLine("UPDATE ID: " + id);
                    Console.WriteLine("UPDATE Old Value : " + oldValue);
                    Console.WriteLine("UPDATE New Value : " + newValue);
                    Console.WriteLine("UPDATE Replica ID: " + reply.ReplicaId);
                    Console.WriteLine("UPDATE Version Number: " + reply.VersionNumber);
                    Console.ResetColor();
                }
                return reply;
            }catch(RpcException e){
                this._lastErrorMessage = e.Message;

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0}", e.Message);
                    Console.ResetColor();
                }
                return null;
            }
        }


        public void crashServer(){
            this._client.crashServerAsync(new DIDAStorage.Proto.DIDACrashServerRequest());
            return;
        }


        public DIDAStorage.Proto.DIDAListServerReply listServer(){
            return this._client.listServer(new DIDAStorage.Proto.DIDAListServerRequest());
        }

        public void ToggleVerbose(){
            this._verbose = !this._verbose;
        }
    }
}