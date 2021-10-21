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
                    return new DIDARecord { id = id, version = dValue.version, val = dValue.value };
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

        public DIDAVersion Write(string id, string val)
        {
            DIDAValue valueToWrite = new DIDAValue();

            valueToWrite.value = val;

            DIDAVersion newVersion = new DIDAVersion
            {
                replicaId = this._replicaId
            };

            CheckIfNewRecord(id);

            lock (this._values[id])
            {
                List<DIDAValue> currentValues = this._values[id];
                //If There are already versions of something
                if (currentValues.Count != 0)
                {
                    int oldestIndex = FindIndexOfOldestVersion(currentValues);
                    //Increment the version
                    newVersion.versionNumber = FindMostRecentVersionNumber(currentValues) + 1;

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
                        versionNumber = 0
                    };

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
        }

        public DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue)
        {
            lock (this._values[id])
            {
                //Give the version -1 so that we get the latest
                var valueToChange = this.Read(id, new DIDAVersion { replicaId = -1, versionNumber = -1 });

                if (valueToChange.val == oldvalue)
                {
                    return this.Write(id, newvalue);
                }

                //TODO: Return error?
                return new DIDAVersion
                {
                    versionNumber = -1,
                    replicaId = -1
                };
            }

        }

        private int FindMostRecentVersionNumber(List<DIDAValue> values)
        {
            DIDAVersion newestVersion = values[0].version;

            foreach (DIDAValue v in values)
            {
                if (v.version > newestVersion)
                {
                    newestVersion = v.version;
                }
            }
            return newestVersion.versionNumber;
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

            var reply = new DIDAStorage.Proto.DIDAListServerReply();
            foreach(KeyValuePair<string, List<DIDAStorage.DIDAValue>> currentRecords in this._values){
                var currentCompleteRecord = new DIDAStorage.Proto.DIDACompleteRecord();
                currentCompleteRecord.Id = currentRecords.Key;
                foreach(DIDAStorage.DIDAValue currentVersion in currentRecords.Value){
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
    }
}
