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

        private List<string> _storages = new List<string>();
        private List<string> _workers = new List<string>();

        private bool _verbose = true;

        private int _currentWorkerOrder = 0;

        private int _idCounter = 0;
        public override async Task<DIDARunApplicationReply> runApplication(DIDARunApplicationRequest request, ServerCallContext context)
        {
            Console.WriteLine("SCHEDULER: RUNNING APPLCIATION");
            this.ParseServers(this._workers, request.Workers.ToList());
            this.ParseServers(this._storages, request.Storages.ToList());
            Console.WriteLine("SCHEDULER: GETTING THE OPERATORS");
            //Get the operators
            var operators = this.ReadApplicationFile(request.FilePath);
            Console.WriteLine("SCHEDULER: SORTING OPERATORS");
            //sort them
            operators = operators.OrderBy(op => op.Item2).ToList();
            Console.WriteLine("SCHEDULER: BUILDING REQUEST");
            
            DIDAWorker.Proto.DIDARequest newRequest = new DIDAWorker.Proto.DIDARequest();
            newRequest.Meta = new DIDAWorker.Proto.DIDAMetaRecord{
                Id = this._idCounter,
            };
            this._idCounter++;
            Console.WriteLine("2");
            newRequest.Input = request.Input;
            Console.WriteLine("3");
            newRequest.Next = 0;
            Console.WriteLine("4");
            newRequest.ChainSize = operators.Count;
            Console.WriteLine("5");

            this.ScheduleOperators(newRequest, operators);
            this.AssignStorageDetails(newRequest);

            Console.WriteLine("SCHEDULER: CONTACTING WORKER");

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


        private List<Tuple<string, int>> ReadApplicationFile(string filePath){
            string[] lines;
            try{
                lines = System.IO.File.ReadAllLines(filePath);
            }
            catch(Exception e){
                Console.WriteLine("SCHEDULER: FAILED TO READ FILE: " + e.ToString());
                Environment.Exit(1);
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
            Console.WriteLine("SCHEDULER PARSE SERVERS");
            Console.WriteLine(nodes.ToString());
            foreach(string node in nodes){
                if(!listToAdd.Contains(node)){
                    listToAdd.Add(node);
                }
                else{
                    if(this._verbose){
                        Console.WriteLine("WARNING: could not parse node: {0}", node);
                    }
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
            Console.WriteLine("SCHEDULER SCEDULING OPERATORS");

            if(this._workers.Count == 0){
                //TODO; Do something?
            }

            foreach(Tuple<string, int> op in operators){
                var hostInfo = this.GetAddressInfo(this._workers[this._currentWorkerOrder]);

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
                this._currentWorkerOrder = this._currentWorkerOrder % this._workers.Count;
            }
        }

        private void AssignStorageDetails(DIDAWorker.Proto.DIDARequest request){
            Console.WriteLine("ASSIGNING STORAGE DETAILS");
            foreach(string storage in this._storages){
                var storageInfo = this.GetAddressInfo(storage);
                request.Meta.Storages.Add(
                    new DIDAWorker.Proto.DIDAStorageNodeDetails{
                        Host = storageInfo.Item1,
                        Port = Int32.Parse(storageInfo.Item2),
                    }
                );
            }
        }
        
    }
    

}