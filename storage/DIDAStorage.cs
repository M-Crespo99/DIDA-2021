using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

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
        private string _host;
        private int _port;

        private string _serverName;

        private Dictionary<string, List<DIDAValue>> _values = new Dictionary<string, List<DIDAValue>>();

        private Dictionary<string, int> _versionNumbers = new Dictionary<string, int>();

        public Storage(int replicaId, bool debug, string host, int port, string serverName)
        {
            this._replicaId = replicaId;
            this._debug = debug;
            this._host = host;
            this._port = port;
            this._serverName = serverName;
        }

        public DIDARecord Read(string id, DIDAVersion version)
        {
            //First check if we have an entry with this ID
            if (this._debug)
            {
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
                    lock (this)
                    {
                        this._numberOfReads++;
                    }

                    return new DIDARecord { id = id, version = dValue.version, val = dValue.value, valueTS = dValue.valueTS };
                }
                if (this._debug)
                {
                    Console.WriteLine("STORAGE: Could not find version of record {0} with: \nVersion Number: {1}\nReplica ID: {2}",
                     id,
                     version.versionNumber,
                     version.replicaId);
                }
                throw (new Exceptions.NoSuchVersionException(id, version));
            }
            else
            {
                if (this._debug)
                {
                    Console.WriteLine("STORAGE: Could not find record with ID: " + id);
                }
                throw (new Exceptions.NoSuchRecordException(id));
            }
        }


        public GossipLib.LamportClock processNewGossipMessage()
        {
            return null;
        }
        public DIDAVersion Write(string id, string val, GossipLib.GossipLogRecord record, bool gossipUpdate)
        {
            try
            {
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
                        


                        newVersion.replicaId = record._replicaId;

                        valueToWrite.valueTS = FindMostRecentValue(id).valueTS;

                        //Merge it with incoming replicaTS
                        valueToWrite.valueTS.merge(record._updateTS);

                        newVersion.replicaTS = mostRecentVersion.replicaTS;
                        newVersion.versionNumber = record._operation.versionNumber;
                        this._versionNumbers.TryAdd(id, newVersion.versionNumber);

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
                        if(gossipUpdate){
                            valueToWrite.version = new DIDAVersion
                                {
                                    replicaId = record._replicaId,
                                    versionNumber = record._operation.versionNumber,
                                    replicaTS = record._updateTS,
                                };
                            valueToWrite.valueTS = record._updateTS;
                            this._versionNumbers.TryAdd(id, record._operation.versionNumber);


                        }else{
                            valueToWrite.version = new DIDAVersion
                            {
                                replicaId = this._replicaId,
                                versionNumber = 0,
                                replicaTS = new GossipLib.LamportClock(this._storageCounter + 1),
                            };
                            this._versionNumbers.TryAdd(id, 0);
                            valueToWrite.valueTS = new GossipLib.LamportClock(this._storageCounter + 1);
                            valueToWrite.version.replicaTS.incrementAt(this._replicaId - 1);
                            valueToWrite.valueTS.incrementAt(this._replicaId - 1);
                        }

                        lock (this)
                        {
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
            }
            catch (Exception e)
            {
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
                    return this.Write(id, newvalue, null, false);
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
            var orderdValues = this._values[id].OrderBy(value => value.version).ToList();
            return orderdValues.Last();
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
            Console.WriteLine("Storage Node {0}:", this._serverName);
            Console.WriteLine("Address: {0}:{1}:", this._host, this._port);

            var reply = new DIDAStorage.Proto.DIDAListServerReply();

            if (this._values.Count == 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("No Records stored in this node.");
                Console.ResetColor();

            }
            lock(this._values){
                foreach (KeyValuePair<string, List<DIDAStorage.DIDAValue>> currentRecords in this._values)
                {
                    Console.WriteLine("Record Key: {0}.", currentRecords.Key);
                    var currentCompleteRecord = new DIDAStorage.Proto.DIDACompleteRecord();
                    currentCompleteRecord.Id = currentRecords.Key;
                    var orderdRecords = currentRecords.Value.OrderBy(value => value.version).ToList();
                    foreach (DIDAStorage.DIDAValue currentVersion in orderdRecords)
                    {
                        if (currentVersion.Equals(orderdRecords.Last()))
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("--> Most Recent Value:");
                        }

                        Console.WriteLine("\tValue: {0}.", currentVersion.value);
                        Console.WriteLine("\tVersion Number: {0}.", currentVersion.version.versionNumber);
                        Console.WriteLine("\tReplica ID: {0}.", currentVersion.version.replicaId);
                        Console.ResetColor();
                        currentCompleteRecord.Versions.Add(new DIDAStorage.Proto.DIDARecordReply
                        {
                            Id = currentRecords.Key,
                            Version = new DIDAStorage.Proto.DIDAVersion
                            {
                                VersionNumber = currentVersion.version.versionNumber,
                                ReplicaId = currentVersion.version.replicaId
                            },
                            Val = currentVersion.value
                        });
                    }
                    reply.Records.Add(currentCompleteRecord);
                }
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

        public GossipLib.LamportClock getReplicaTimestamp(string id)
        {
            //If the record doesnt exist on this replica, we return [0 0 1] (if replica 3)
            if (!this._values.ContainsKey(id))
            {
                var clock = new GossipLib.LamportClock(this._storageCounter + 1);
                clock.incrementAt(this._replicaId - 1);
                return clock;
            }
            else
            {
                lock (this._values[id])
                {
                    //Return the lamport clock on the key
                    var mostRecentVersion = FindMostRecentVersion(this._values[id]);
                    return mostRecentVersion.replicaTS;
                }
            }
        }

        public void incrementReplicaTSOnRecord(string id)
        {
            lock(this._values){
                if (!this._values.ContainsKey(id))
                {

                    this._values.Add(id, new List<DIDAValue>());

                    lock (this._values[id])
                    {
                        DIDAVersion newVersion = new DIDAVersion
                        {
                            versionNumber = -1,
                            replicaId = this._replicaId,
                            replicaTS = new GossipLib.LamportClock(this._storageCounter + 1)
                        };
                        this._versionNumbers.TryAdd(id,0);
                        this._values[id].Add(new DIDAValue
                        {
                            value = "",
                            valueTS = newVersion.replicaTS,
                            version = newVersion
                        });
                        this._values[id].First().version.replicaTS.incrementAt(this._replicaId - 1);
                    }
                    return;
                }
                lock (this._values[id])
                {
                    this.FindMostRecentValue(id).version.replicaTS.incrementAt(this._replicaId - 1);
                }
            }
        }

        public GossipLib.LamportClock getValueTS(string id)
        {
            lock (this._values)
            {
                if (this._values.ContainsKey(id))
                {
                    lock (this._values[id])
                    {
                        var value = this.FindMostRecentValue(id);
                        return this.FindMostRecentValue(id).valueTS;
                    }
                }
                else
                {
                    return new GossipLib.LamportClock(this._storageCounter + 1);
                }
            }

        }

        public void printStatus()
        {
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
            Console.WriteLine("Storage Node Status:");
            Console.WriteLine("\tName: {0}.", this._serverName);
            Console.WriteLine("\tID: {0}.", this._replicaId);
            Console.WriteLine("\tStatus: Running.");
            Console.WriteLine("\tPerformed {0} Read Operations.", this._numberOfReads);
            Console.WriteLine("\tPerformed {0} Write Operations.", this._numberOfWrites);

            int counter = 0;
            List<string> l = new List<string>();
            lock (this)
            {
                foreach (KeyValuePair<string, List<DIDAStorage.DIDAValue>> currentRecords in this._values)
                {
                    counter += currentRecords.Value.Count;
                    l.Add(currentRecords.Key);
                }
            }
            Console.WriteLine("\tNumber of Records Stored: {0}.", counter);
            Console.Write("\tStored Keys: ");
            foreach (string s in l)
            {
                if (s == l.Last())
                {
                    Console.Write(s + ".");
                }
                else
                {
                    Console.Write(s + ", ");
                }

            }
            Console.WriteLine();
            Console.WriteLine("%%%%%%%%%%%%%%%%%%%%%");
        }

        public bool toggleDebug()
        {
            this._debug = !this._debug;
            return this._debug;
        }

        public bool hasRecord(string id)
        {
            return this._values.ContainsKey(id);
        }
        public void incrementStorages()
        {
            lock (this)
            {
                this._storageCounter++;
            }
        }

        public int getNextVersionNumber(string id){
            if(this._values.ContainsKey(id)){
                lock(this._values[id]){
                    this._versionNumbers[id]+= 1;
                    return this._versionNumbers[id];
                }
            }
            else{
                this._versionNumbers.TryAdd(id, 0);
            }
            return 0;
        }
    }
}
