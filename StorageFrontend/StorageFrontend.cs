using Grpc.Core;
using Grpc.Net.Client;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace StorageFrontend
{
    public class StorageFrontend
    {

        Dictionary<string, LamportClock> _clocks;

        private int _port;

        private string _host;
        private GrpcChannel _channel;
        private DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient _client;

        private string _lastErrorMessage = "";

        private bool _verbose = false;

        private int _numberOfStorages = 0;

        public StorageFrontend(string host, int port, int numberOfStorages)
        {
            this._port = port;
            this._host = host;

            this._numberOfStorages = numberOfStorages;

            this._verbose = false;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);

            this._clocks = new Dictionary<string, LamportClock>();
        }

        public StorageFrontend(string host, int port, int numberOfStorages, bool verbose)
        {
            this._port = port;
            this._host = host;

            this._numberOfStorages = numberOfStorages;


            this._verbose = verbose;

            this._channel = GrpcChannel.ForAddress("http://" + host + ":" + port);

            this._client = new DIDAStorage.Proto.DIDAStorageService.DIDAStorageServiceClient(this._channel);

            this._clocks = new Dictionary<string, LamportClock>();
        }

        public async Task<DIDAStorage.Proto.StatusReply> printStatus()
        {
            return await this._client.statusAsync(new DIDAStorage.Proto.StatusRequest());
        }

        public DIDAStorage.Proto.DIDAVersion Write(string id, string value)
        {

            LamportClock lClock;

            //Check if we have a value for that clock
            if(!this._clocks.ContainsKey(id)){
                this._clocks[id] = new LamportClock(this._numberOfStorages);
            }
            
            lClock = this._clocks[id];

            var rand = new Random();
            
            DIDAStorage.Proto.DIDAWriteRequest writeRequest = new DIDAStorage.Proto.DIDAWriteRequest
            {
                Id = id,
                Val = value,
                Clock = LClockToProto(lClock),
                UniqueID = rand.Next()
            };


            try{
                var reply = this._client.write(writeRequest);

                Console.WriteLine("RAW RESPONSE: " + reply.Clock.ToString());

                this._clocks[id] =  protoToLClock(reply.Clock);

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("%% Write Operation complete on Storage node at " + this._host + ":" + this._port + " %%");
                    Console.WriteLine("WRITE ID: " + id);
                    Console.WriteLine("WRITE Value: " + value);
                    Console.WriteLine("New Version Number: " + reply.VersionNumber);
                    Console.WriteLine("Replica ID: " + reply.ReplicaId);
                    Console.WriteLine("WRITE  Lamport Clock: " + this._clocks[id].ToString());

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

            LamportClock lClock;

            //Check if we have a value for that clock
            if(!this._clocks.ContainsKey(id)){
                this._clocks[id] = new LamportClock(this._numberOfStorages);
            }
            
            lClock = this._clocks[id];


            //Send the clock on the request
            DIDAStorage.Proto.DIDAReadRequest readRequest = new DIDAStorage.Proto.DIDAReadRequest
            {
                Id = id,
                Version = version,
                Clock = LClockToProto(lClock)
            };


            try{
                var reply = this._client.read(readRequest);
                

                //Add the new clock to the map
                this._clocks[id].merge(this.protoToLClock(reply.Version.Clock));

                if(this._verbose){
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("%% Reading from Storage node at " + this._host + ":" + this._port + " %%");
                    Console.WriteLine("READ ID: " + reply.Id);
                    Console.WriteLine("READ Value : " + reply.Val);
                    Console.WriteLine("READ Replica ID: " + reply.Version.ReplicaId);
                    Console.WriteLine("READ Version Number: " + reply.Version.VersionNumber);
                    Console.WriteLine("READ Lamport Clock: " + this._clocks[id].ToString());

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


        public void gossip(List<GossipLogRecord> records){

            var gossipMessage = new DIDAStorage.Proto.GossipMessage();

            foreach(var record in records){
                var newProtoEntry = GRecordToGProtoRecord(record);


                gossipMessage.Log.Add(newProtoEntry);
            }

            this._client.gossipAsync(gossipMessage);

            return;
        }        

        private LamportClock protoToLClock(DIDAStorage.Proto.LamportClock protoClock){
            List<int> l = new List<int>();

            foreach(var value in protoClock.Values){
                l.Add(value);
            }

            return new LamportClock(l);
        }

        private DIDAStorage.Proto.LamportClock LClockToProto(LamportClock c){
            DIDAStorage.Proto.LamportClock protoLClock = new DIDAStorage.Proto.LamportClock();

            var l = c.toList();

            foreach(var value in l){
                protoLClock.Values.Add(value);
            }

            return protoLClock;
        }

        private DIDAStorage.Proto.GossipLogEntry GRecordToGProtoRecord(GossipLogRecord entry){
            var protoEntry = new DIDAStorage.Proto.GossipLogEntry{
                ReplicaID = entry._replicaId,
                UpdateTS = LClockToProto(entry._updateTS),
                PreviousClock = LClockToProto(entry._prev),
                UpdateIdentifier = entry._operationIdentifier,
                Operation = new DIDAStorage.Proto.GossipOperation{
                    Key = entry._operation.key,
                    NewValue = entry._operation.newValue
                },

                ReplicaTS = LClockToProto(entry._replicaTS)
            };
            return protoEntry;
        }
    }

        public enum operationType{
        READ = 1,
        WRITE = 2,
        UPDATE_IF_VALUE_IS = 3,
    }
    public struct operation{
        public string key;
        public operationType opType;

        public string  newValue;

        public override string ToString()
        {
            return String.Format("K: {0} V: {1}", key, newValue);
        }

    }
    public class GossipLogRecord{
        public int _replicaId;

        public LamportClock _updateTS;

        public LamportClock _prev;

        public LamportClock _replicaTS;

        public int _operationIdentifier;

        public operation _operation;


        public GossipLogRecord(int replicaId,
                                LamportClock updateTS,
                                LamportClock prev,
                                LamportClock replicaTS,
                                int operationIdentifier,
                                operation op){
            

            this._replicaId = replicaId;
            this._prev = prev;
            this._updateTS = updateTS;
            this._operationIdentifier = operationIdentifier;
            this._operation = op;
            this._replicaTS = replicaTS;
        }

        public override string ToString()
        {
            return String.Format("LogRecord: <{0}, {1}, {2}, {3}, {4}> {5}\n", this._replicaId,
                                                                                _updateTS.ToString(),
                                                                                _operation,
                                                                                _prev.ToString(),
                                                                                _operationIdentifier,
                                                                                _replicaTS.ToString());
        }

    }
    public class LamportClock
    {
        private int _numberOfReplicas {get;}
        private int[] _clock {get;}

        public LamportClock(int numberOfReplicas){
            this._clock = new int[numberOfReplicas];
            this._numberOfReplicas = numberOfReplicas;
            for(int i = 0; i < numberOfReplicas; i++){
                this._clock[i] = 0;
            }
        }

        public LamportClock(List<int> list){
            this._clock = new int[list.Count];
            this._numberOfReplicas = list.Count;
            for(int i = 0; i < this._numberOfReplicas; i++){
                this._clock[i] = list[i];
            }
        }

        public LamportClock DeepCopy(){
            var copy = new LamportClock(this._numberOfReplicas);


            for(int i = 0; i < this._numberOfReplicas; i++){
                copy.assign(i, this.At(i));
            }

            return copy;
        }

        public LamportClock(int[] clock){
            this._clock = clock;
            this._numberOfReplicas = clock.Length;
        }


        public void merge(LamportClock otherClock){
            for(int i = 0; i < this._numberOfReplicas; i++){
                if(otherClock.At(i) > this._clock[i]){
                    this._clock[i] = otherClock.At(i);
                }
                else if(otherClock.At(i) < this._clock[i]){
                    otherClock.assign(i, this._clock[i]);
                }
            }
        }

        public int assign(int position, int newValue){
            if(position > this._numberOfReplicas || position < 0){
                return -1; 
            }
            this._clock[position] = newValue;
            return newValue;
        }


        public int incrementAt(int position){
            if(position > this._numberOfReplicas || position < 0){
                return -1; 
            }

            this._clock[position] = this._clock[position] + 1;

            return this._clock[position];
        }

        public override string ToString()
        {
            string str = "[ ";

            foreach(int i in this._clock){
                str += i.ToString() + " ";
            }
            return str + "]";
        }

        public int At(int i){
            if(i < 0 || i > this._numberOfReplicas){
                return -1;
            }
            return this._clock[i];
        }

        public List<int> toList(){
            var listToReturn = new List<int>();

            foreach(int i in this._clock){
                listToReturn.Add(i);
            }
            return listToReturn;
        }

        public static bool operator >=(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) < l2.At(i))){
                    return false;
                }
            }
            return true;
        }
        public static bool operator <=(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) > l2.At(i))){
                    return false;
                }
            }
            return true;
        }

        public bool isConcurrent(LamportClock otherClock){
            return (!(this >= otherClock) && !(this <= otherClock)); 
        }
    }

}