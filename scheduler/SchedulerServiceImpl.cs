using System;
using System.Threading.Tasks;
using Grpc.Core;
using System.Collections.Generic;
using static DIDASchedulerService;
using System.Linq;

namespace scheduler
{
    public class SchedulerServiceImpl : DIDASchedulerServiceBase
    {

        private String _host;
        private int _port;

        private List<string> _storages = new List<string>();
        private List<string> _workers = new List<string>();
        private int _currentWorkerOrder = 0;

        private int _idCounter = 0;

        public SchedulerServiceImpl(String host, int port)
        {
            this._host = host;
            this._port = port;
        }
        public override async Task<DIDARunApplicationReply> runApplication(DIDARunApplicationRequest request, ServerCallContext context)
        {
            Console.WriteLine("ENTERED SCHEDULER");
            this.ParseServers(this._workers, request.Workers.ToList());
            this.ParseServers(this._storages, request.Storages.ToList());
            //Get the operators
            var operators = this.ReadApplicationFile(request.FilePath);
            //sort them

            if(operators == null){
                return await Task.FromResult(new DIDARunApplicationReply{Ok = false});
            }
            operators = operators.OrderBy(op => op.Item2).ToList();
            
            DIDAWorker.Proto.DIDARequest newRequest = new DIDAWorker.Proto.DIDARequest();
            lock(this){
                newRequest.Meta = new DIDAWorker.Proto.DIDAMetaRecord{
                    Id = this._idCounter,
                    SchedulerHost = this._host,
                    SchedulerPort = this._port
                };
                this._idCounter++;
            }
            
            newRequest.Input = request.Input;
            newRequest.Next = 0;
            newRequest.ChainSize = operators.Count;

            this.ScheduleOperators(newRequest, operators);
            this.AssignStorageDetails(newRequest);


            WorkerFrontend.Frontend workerFrontend = new WorkerFrontend.Frontend(newRequest.Chain.First().Host, newRequest.Chain.First().Port);

            workerFrontend.workOnOperator(newRequest);            
            
            return await Task.FromResult(new DIDARunApplicationReply{Ok = true});
        }

        public override async Task<DIDAAssignmentReply> assignOperator(DIDAAssignmentRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for assign operator ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new DIDAAssignmentReply{Ok = true});
        }

        public override async Task<LivenessCheckReply> livenessCheck(LivenessCheckRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing liveness check for scheduler##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");
            return await Task.FromResult(new LivenessCheckReply{Ok = true});
        }

        public override Task<CompleteOperatorReply> operatorComplete(CompleteOperatorRequest request, ServerCallContext context)
        {

            return Task.FromResult(new CompleteOperatorReply { });
        }

        private List<Tuple<string, int>> ReadApplicationFile(string filePath){
            string[] lines;
            
            var argument = Environment.CurrentDirectory.
                    Replace("PCS", "scheduler").Replace("PuppetMaster", "scheduler");

            string fileDir =  String.Format("{0}/scripts/operator_scripts/", argument);

            filePath = fileDir + filePath;

            try{
                lines = System.IO.File.ReadAllLines(filePath);
            }
            catch(Exception ){
                Console.WriteLine("SCHEDULER: Failed to read file application file:\n " + filePath);
                return null;
            }
            var listToReturn = new List<Tuple<string, int>>();
            foreach(string line in lines){
                var parts = line.Split(" ");

                if(parts[0] == "operator" && parts.Length == 3){
                    var currentTuple = new Tuple<string, int>(parts[1], Int32.Parse(parts[2]));
                    listToReturn.Add(currentTuple);
                }
            }
            return listToReturn;
        }

        private void ParseServers(List<string> listToAdd ,List<string> nodes){
            foreach(string node in nodes){
                if(!listToAdd.Contains(node)){
                    listToAdd.Add(node);
                }
            }
        }

        private Tuple<string, string> GetAddressInfo(string URL){
            var parts = URL.Split(":");
            if(parts.Length == 2){
                return new Tuple<string, string>(parts[0], parts[1]);
            }
            return null;
        }

        private void ScheduleOperators(DIDAWorker.Proto.DIDARequest request, List<Tuple<string, int>> operators){
            //Round robin implementation of load distribution

            if(this._workers.Count == 0){
                //TODO; Do something?
            }

            foreach(Tuple<string, int> op in operators){
                Tuple<string, string> hostInfo; 
                lock(this){
                    hostInfo = this.GetAddressInfo(this._workers[this._currentWorkerOrder]);
                }

                var assignment = new DIDAWorker.Proto.DIDAAssignment{
                    Operator = new DIDAWorker.Proto.DIDAOperatorID{
                        Classname = op.Item1,
                        Order = op.Item2,
                    },
                    Host = hostInfo.Item1,
                    Port = Int32.Parse(hostInfo.Item2),
                    Output = ""
                };
                request.Chain.Add(assignment);
                lock(this){
                    this._currentWorkerOrder = (this._currentWorkerOrder + 1) % this._workers.Count;
                }
            }
        }

        private void AssignStorageDetails(DIDAWorker.Proto.DIDARequest request){
            int counter = 1;
            foreach(string storage in this._storages){
                var storageInfo = this.GetAddressInfo(storage);
                request.Meta.Storages.Add(
                    new DIDAWorker.Proto.DIDAStorageNodeDetails{
                        Host = storageInfo.Item1,
                        Port = Int32.Parse(storageInfo.Item2),
                        Id = counter.ToString() //TODO CHANGE THIS: MIXUP WITH SERVER 
                    }
                );
            }

        }
    }
    

}