using Grpc.Core;
using Grpc.Net.Client;
using System;

namespace WorkerFrontend
{
    public class Frontend
    {
        private int _port;

        private string _host;
        private GrpcChannel _channel;
        private DIDAWorker.Proto.DIDAWorkerService.DIDAWorkerServiceClient _client;

        private string _lastErrorMessage = "";

        private bool _verbose = false;

        public Frontend(string host, int port)
        {
            this._port = port;
            this._host = host;

            this._verbose = false;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAWorker.Proto.DIDAWorkerService.DIDAWorkerServiceClient(this._channel);
        }

        public Frontend(string host, int port, bool verbose)
        {
            this._port = port;
            this._host = host;

            this._verbose = verbose;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAWorker.Proto.DIDAWorkerService.DIDAWorkerServiceClient(this._channel);
        }

        public void workOnOperator(DIDAWorker.Proto.DIDARequest request){
            this._client.workOnOperatorAsync(request); //Do we care about this return value?
        }

        public string getLastErrorMessage(){
            return this._lastErrorMessage;
        }

        public void crashServer(){
            throw new NotImplementedException();
        }
        public void ToggleVerbose(){
            this._verbose = !this._verbose;
        }
    }
}
