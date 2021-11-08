using System;
using System.Collections.Generic;
using System.Linq;


namespace DIDAStorage
{
    public class Storage : IDIDAStorage
    {
        private int MAX_VERSIONS = 10;

        private bool _debug = false;

        private int _replicaId = 0;

        private int _numberOfWrites = 0;
        private int _numberOfReads = 0;

        private int _storageCounter = 0;

        private Dictionary<string, List<DIDAValue>> _values = new Dictionary<string, List<DIDAValue>>();

        public Storage(int replicaId, bool debug)
        {
            this._replicaId = replicaId;
            this._debug = debug;
        }

        public DIDARecord Read(string id, DIDAVersion version)
        {
            //First check if we have an entry with this ID
            if(this._debug){
                Console.WriteLine("\nSTORAGE: Processing Read Request with the following parameters: ");
                Console.WriteLine("ID: " + id);
                Console.WriteLine("Version Number: " + version.versionNumber);
                Console.WriteLine("Replica ID: " + version.replicaId);
            }
            if (this._values.ContainsKey(id))
            {
                DIDAValue dValue = FindValue(id, version);
                if (dValue.value != null)
                {
                    lock(this){
                        this._numberOfReads++;
                    }
                    Console.WriteLine("Version: ");
                    Console.WriteLine(dValue.version.ToString());

                    return new DIDARecord { id = id, version = dValue.version, val = dValue.value, valueTS = dValue.valueTS};
                }
                if(this._debug){
                    Console.WriteLine("STORAGE: Could not find version of record {0} with: \nVersion Number: {1}\nReplica ID: {2}",
                     id, 
                     version.versionNumber, 
                     version.replicaId);
                }
                throw (new Exceptions.NoSuchVersionException(id, version));
            }
            else
            {
                if(this._debug){
                    Console.WriteLine("STORAGE: Could not find record with ID: " + id);
                }
                throw (new Exceptions.NoSuchRecordException(id));
            }
        }


        public GossipLib.LamportClock processNewGossipMessage(){
            return null;
        }
        public DIDAVersion Write(string id, string val, GossipLib.LamportClock replicaTSToMerge)
        {
            try{
            DIDAValue valueToWrite = new DIDAValue();

            valueToWrite.value = val;

            CheckIfNewRecord(id);


            lock (this._values[id])
            {
                List<DIDAValue> currentValues = this._values[id];
                //If There are already versions of something

                DIDAVersion newVersion = new DIDAVersion
                {
                    replicaId = this._replicaId
                };
                if (currentValues.Count != 0)
                {
                    int oldestIndex = FindIndexOfOldestVersion(currentValues);
                    //Increment the version
                    var mostRecentVersion = FindMostRecentVersion(currentValues);


                    newVersion.replicaId = mostRecentVersion.replicaId;

                    valueToWrite.valueTS = FindMostRecentValue(id).valueTS;

                    //Merge it with incoming replicaTS
                    valueToWrite.valueTS.merge(replicaTSToMerge);
                    
                    newVersion.replicaTS = mostRecentVersion.replicaTS;
                    newVersion.versionNumber = mostRecentVersion.versionNumber + 1;

                    valueToWrite.version = newVersion;

                    //Write on top of the oldest if we already at MAX VERSIONS
                    if (currentValues.Count == MAX_VERSIONS)
                    {
                        currentValues[oldestIndex] = valueToWrite;
                    }
                    else
                    {
                        //Simply add to the existing versions
                        currentValues.Add(valueToWrite);
                    }
                }
                else
                {
                    //If it is a new Record
                    valueToWrite.version = new DIDAVersion
                    {
                        replicaId = this._replicaId,
                        versionNumber = 0,
                        replicaTS = new GossipLib.LamportClock(this._storageCounter + 1),
                    };
                    valueToWrite.valueTS = new GossipLib.LamportClock(this._storageCounter + 1);
                    valueToWrite.version.replicaTS.incrementAt(this._replicaId - 1);
                    valueToWrite.valueTS.incrementAt(this._replicaId - 1);
                    lock(this){
                        this._numberOfWrites++;
                    }
                    currentValues.Add(valueToWrite);
                }
            }
            if (this._debug)
            {
                Console.WriteLine("--> New Record Written: ");
                Console.WriteLine("ID: " + id);
                Console.WriteLine(valueToWrite);
            }
            return valueToWrite.version;
            }catch (Exception e){
                Console.WriteLine(e.ToString());
                return new DIDAVersion();
            }
        }

        public DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue)
        {
            lock (this._values[id])
            {
                //Give the version -1 so that we get the latest
                var valueToChange = this.Read(id, new DIDAVersion { replicaId = -1, versionNumber = -1 });

                if (valueToChange.val == oldvalue)
                {
                    return this.Write(id, newvalue, new GossipLib.LamportClock(this._storageCounter));
                }

                //TODO: Return error?
                return new DIDAVersion
                {
                    versionNumber = -1,
                    replicaId = -1
                };
            }

        }

        private DIDAVersion FindMostRecentVersion(List<DIDAValue> values)
        {
            DIDAVersion newestVersion = values[0].version;

            foreach (DIDAValue v in values)
            {
                if (v.version > newestVersion)
                {
                    newestVersion = v.version;
                }
            }
            return newestVersion;
        }

        private DIDAValue FindMostRecentValue(string id)
        {
            int maxVersionNumber = this._values[id].Max(value => value.version.versionNumber);

            return this._values[id].Find(value => value.version.versionNumber == maxVersionNumber);
        }


        private int FindIndexOfOldestVersion(List<DIDAValue> values)
        {
            int indexOfOldest = 0;

            DIDAVersion oldestVersion = values[0].version;

            foreach (DIDAValue v in values)
            {
                if (v.version < oldestVersion)
                {
                    indexOfOldest = values.IndexOf(v);
                    oldestVersion = v.version;
                }
            }
            return indexOfOldest;
        }

        private DIDAValue FindValue(string id, DIDAVersion version)
        {
            lock (this._values[id])
            { //We only need to lock the list of values we are accessing
                if (version.versionNumber < 0)
                {
                    return FindMostRecentValue(id);
                }
                return this._values[id].Find(value => value.version == version);
            }
        }
        public DIDAStorage.Proto.DIDAListServerReply getProtoRecords()
        {
            
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
            Console.WriteLine("Storage Node {0}:", this._replicaId);

            var reply = new DIDAStorage.Proto.DIDAListServerReply();

            if(this._values.Count == 0){
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("No Records stored in this node.");
                Console.ResetColor();

            }

            foreach(KeyValuePair<string, List<DIDAStorage.DIDAValue>> currentRecords in this._values){
                Console.WriteLine("Record Key: {0}.", currentRecords.Key);
                var currentCompleteRecord = new DIDAStorage.Proto.DIDACompleteRecord();
                currentCompleteRecord.Id = currentRecords.Key;
                foreach(DIDAStorage.DIDAValue currentVersion in currentRecords.Value){
                    if(currentVersion .Equals(currentRecords.Value.Last())){
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("--> Most Recent Value:");
                    }

                    Console.WriteLine("\tValue: {0}.", currentVersion.value);
                    Console.WriteLine("\tVersion Number: {0}.", currentVersion.version.versionNumber);
                    Console.WriteLine("\tReplica ID: {0}.", currentVersion.version.replicaId);
                    Console.ResetColor();
                    currentCompleteRecord.Versions.Add(new DIDAStorage.Proto.DIDARecordReply{
                        Id = currentRecords.Key,
                        Version = new DIDAStorage.Proto.DIDAVersion{
                            VersionNumber = currentVersion.version.versionNumber,
                            ReplicaId = currentVersion.version.replicaId
                        },
                        Val = currentVersion.value
                    });
                }
                reply.Records.Add(currentCompleteRecord);
            }
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
            return reply;
        }

        private void CheckIfNewRecord(string id)
        {
            lock (this._values)
            {
                if (!this._values.ContainsKey(id))
                {
                    this._values.Add(id, new List<DIDAValue>());
                }
            }
        }

        public GossipLib.LamportClock getReplicaTimestamp(string id){
            if(!this._values.ContainsKey(id)){
                    var clock = new GossipLib.LamportClock(this._storageCounter);
                    clock.incrementAt(this._replicaId - 1);
                    return clock;
            }
            else{
                lock(this._values[id]){
                    var mostRecentVersion = FindMostRecentVersion(this._values[id]);
                    return mostRecentVersion.replicaTS;
                }
            }
        }

        public void incrementReplicaTSOnRecord(string id){
            lock(this._values[id]){
                this.FindMostRecentValue(id).version.replicaTS.incrementAt(this._replicaId - 1);
            }
        }


        public void printStatus(){
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
            Console.WriteLine("Storage Node Status:");
            Console.WriteLine("\tID: {0}.", this._replicaId);
            Console.WriteLine("\tStatus: Running.");
            Console.WriteLine("\tPerformed {0} Read Operations.", this._numberOfReads);
            Console.WriteLine("\tPerformed {0} Write Operations.", this._numberOfWrites);
            
            int counter = 0;
            List<string> l = new List<string>();
            lock(this){
                foreach(KeyValuePair<string, List<DIDAStorage.DIDAValue>> currentRecords in this._values){
                    counter+= currentRecords.Value.Count;
                    l.Add(currentRecords.Key);
                }
            }
            Console.WriteLine("\tNumber of Records Stored: {0}.", counter);
            Console.Write("\tStored Keys: ");
            foreach(string s in l){
                if(s == l.Last()){
                    Console.Write(s + ".");
                }else{
                    Console.Write(s + ", ");
                }

            }
            Console.WriteLine();
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
        }

        public bool toggleDebug(){
            this._debug = !this._debug;
            return this._debug;
        }

        public bool hasRecord(string id){
            return this._values.ContainsKey(id);
        }
        public void incrementStorages(){
            this._storageCounter++;
        }
    }
}
