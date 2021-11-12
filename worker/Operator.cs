using System;
using System.Threading;
using DIDAWorker;

namespace DIDAOperator
{

    public class UpdateAndChainOperator : IDIDAOperator
    {
        private IDIDAStorage proxy;

        public UpdateAndChainOperator()
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        // this operator increments the storage record identified in the metadata record every time it is called.
        string IDIDAOperator.ProcessRecord(DIDAMetaRecord meta, string input, string previousOperatorOutput)
        {
            var val = proxy.updateIfValueIs(new DIDAUpdateIfRequest
                {Id = input, Newvalue = "success", Oldvalue = previousOperatorOutput});
            val = proxy.updateIfValueIs(new DIDAUpdateIfRequest
                {Id = input, Newvalue = "failure", Oldvalue = previousOperatorOutput});

            Console.WriteLine("updated record:" + input);
            return "end";
        }

        void IDIDAOperator.ConfigureStorage(IDIDAStorage proxy)
        {
            this.proxy = proxy;
        }
    }

    public class AddOperator : IDIDAOperator
    {
        private IDIDAStorage proxy;

        public AddOperator()
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        // this operator increments the storage record identified in the metadata record every time it is called.
        string IDIDAOperator.ProcessRecord(DIDAMetaRecord meta, string input, string previousOperatorOutput)
        {
            Console.WriteLine("sleeping now");
            System.Threading.Thread.Sleep(5000);
            Console.WriteLine("input string was: " + input);
            Console.Write("reading data record: " + input + " with value: ");
            var val = proxy.read(new DIDAReadRequest
                {Id = input, Version = new DIDAVersion {VersionNumber = -1, ReplicaId = -1}});
            string storedString = val.Val;
            Console.WriteLine(storedString);
            int requestCounter;
            string output;
            try
            {
                requestCounter = Int32.Parse(storedString);
                requestCounter++;
                output = requestCounter.ToString();
            }
            catch (Exception e)
            {
                output = "int_conversion_failed";
                Console.WriteLine(" operator expecting int but got chars: " + e.Message);
            }


            int oneAhead = Int32.Parse(input);
            oneAhead++;
            Console.Write("reading data record: " + oneAhead + " with value: ");
            val = proxy.read(new DIDAReadRequest
                {Id = oneAhead.ToString(), Version = new DIDAVersion {VersionNumber = -1, ReplicaId = -1}});
            storedString = val.Val;
            Console.WriteLine(storedString);


            proxy.write(new DIDAWriteRequest {Id = input, Val = output});
            Console.WriteLine("writing data record:" + input + " with new value: " + output);
            return output;
        }

        void IDIDAOperator.ConfigureStorage(IDIDAStorage proxy)
        {
            this.proxy = proxy;
        }
    }

    public class IncrementOperator : IDIDAOperator
    {
        private IDIDAStorage proxy;

        public IncrementOperator()
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        // this operator increments the storage record identified in the metadata record every time it is called.
        string IDIDAOperator.ProcessRecord(DIDAMetaRecord meta, string input, string previousOperatorOutput)
        {
            Console.WriteLine("input string was: " + input);
            Console.Write("reading data record: " + meta.Id + " with value: ");
            var val = proxy.read(new DIDAReadRequest
                {Id = meta.Id.ToString(), Version = new DIDAVersion {VersionNumber = -1, ReplicaId = -1}});
            string storedString = val.Val;
            Console.WriteLine(storedString);
            int requestCounter = Int32.Parse(storedString);

            requestCounter++;
            //requestCounter += Int32.Parse(previousOperatorOutput);

            proxy.write(new DIDAWriteRequest {Id = meta.Id.ToString(), Val = requestCounter.ToString()});
            Console.WriteLine("writing data record:" + meta.Id + " with new value: " + requestCounter.ToString());
            return requestCounter.ToString();
        }

        void IDIDAOperator.ConfigureStorage(IDIDAStorage proxy)
        {
            this.proxy = proxy;
        }
    }
}