using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using static PCSService;

namespace PCS
{
    public class PCSServiceImpl : PCSServiceBase
    {
        private static readonly int NumProcs = Environment.ProcessorCount;
        private static readonly int ConcurrencyLevel = NumProcs * 2;
        private static int Counter = 5000;
        
        private ConcurrentDictionary<int, string> _portWorker = new (ConcurrencyLevel, 100);
        private ConcurrentDictionary<int, string> _portStorage = new (ConcurrencyLevel, 100);
        public override async Task<PCSRunWorkerReply> runWorker(PCSRunWorkerRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Worker ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                var counter = Interlocked.Increment(ref Counter);
                _portWorker.TryAdd(counter, String.Format("Worker-{0}", counter));
                
                //TODO must change the path below (just for testing now)
                var argument = String.Format("/Users/wallacegarbim/IST/REPO/DIDA-2021/worker/bin/Debug/net5.0/worker.dll {0}",
                    counter);
                
                executeRunCommand("/usr/local/share/dotnet/dotnet", argument);
                return await Task.FromResult(new PCSRunWorkerReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunWorkerReply {Ok = false});
            }
        }

        private void executeRunCommand(string fileName, string argument)
        {
            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                ProcessStartInfo startInfo = new ProcessStartInfo { FileName = fileName, Arguments = argument}; 
                Process proc = new Process() { StartInfo = startInfo, };
                proc.Start();
            }
        }

        public override async Task<PCSRunStorageReply> runStorage(PCSRunStorageRequest request, ServerCallContext context)
        {
            Console.WriteLine("## Testing parameters for Run Storage ##");
            Console.WriteLine(request.ToString());
            Console.WriteLine("## ------ ##");

            try
            {
                var counter = Interlocked.Increment(ref Counter);
                _portStorage.TryAdd(counter, String.Format("Storage-{0}", counter));
                
                var argument = String.Format("/Users/wallacegarbim/IST/REPO/DIDA-2021/storage/bin/Debug/net5.0/storage.dll {0}",
                    counter);
                executeRunCommand("/usr/local/share/dotnet/dotnet", argument);
                return await Task.FromResult(new PCSRunStorageReply {Ok = true});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return await Task.FromResult(new PCSRunStorageReply {Ok = false});
            }
        }
    }
}