using System;
using System.Collections.Generic;


namespace DIDAStorage{
    public class DIDAStorage : IDIDAStorage {
		private int MAX_VERSIONS = 10;

        private bool debug = true;

		private int replicaId = 0;
		private Dictionary<string, List<DIDAValue>> _values = new Dictionary<string, List<DIDAValue>>();

		public DIDAStorage(int replicaId){
			this.replicaId = replicaId;
		}

		public DIDARecord Read(string id, DIDAVersion version){
			if(this._values.ContainsKey(id)){
                lock(this._values[id]){
                    DIDAValue dValue =  FindValue(id, version);
                    if(dValue.value != null){
                        return new DIDARecord{id = id, version = version, val = dValue.value};
                    }
                }
				throw(new Exceptions.NoSuchVersionException(id, version));
			}
			else{
				throw(new Exceptions.NoSuchRecordException(id));
			}
		}

		public DIDAVersion Write(string id, string val){
			DIDAValue valueToWrite = new DIDAValue();
		
			valueToWrite.value = val;

			DIDAVersion newVersion = new DIDAVersion{
				replicaId = this.replicaId
			};

            CheckIfNewRecord(id);

            lock(this._values[id]){
			    List<DIDAValue> currentValues = this._values[id];
			    //If There are already versions of something
			    if(currentValues.Count != 0){
				    int oldestIndex = FindIndexOfOldestVersion(currentValues);
				    //Increment the version
				    newVersion.versionNumber = FindMostRecentVersion(currentValues) + 1;

				    valueToWrite.version = newVersion;

				    //Write on top of the oldest	
				    if(currentValues.Count == MAX_VERSIONS){
					    currentValues[oldestIndex] = valueToWrite;

				    }else{
                        //Simply add to the existing versions
					    currentValues.Add(valueToWrite);
				    }
			    }else{
				    //If it is a new Record
				    valueToWrite.version = new DIDAVersion{
					replicaId = replicaId,
					versionNumber = 0
				    };

				    currentValues.Add(valueToWrite);
                }
            }
            if(debug){
                Console.WriteLine("--> New Record Written: ");
                Console.WriteLine("ID: "+ id);
                Console.WriteLine(valueToWrite);
            }
			return valueToWrite.version;
		}
		
		public DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue){
			throw new NotImplementedException();
		}

		private int FindMostRecentVersion(List<DIDAValue> values){
			DIDAVersion newestVersion = values[0].version;

			foreach(DIDAValue v in values){
				if(v.version > newestVersion){
					newestVersion = v.version;
				}
			}
			return newestVersion.versionNumber; 
		}
		private int FindIndexOfOldestVersion(List<DIDAValue> values){
			int indexOfOldest = 0;
        
			DIDAVersion oldestVersion = values[0].version;

			foreach(DIDAValue v in values){
				if(v.version < oldestVersion){
					indexOfOldest = values.IndexOf(v);
					oldestVersion = v.version;
				}
			}
			return indexOfOldest; 
		}

        private DIDAValue FindValue(string id, DIDAVersion version){
            lock(this._values[id]){ //We only need to lock the list of values we are accessing
                return this._values[id].Find(value => value.version == version);
            }
        }

        private void CheckIfNewRecord(string id){
            lock(this._values){
                if(!this._values.ContainsKey(id)){
				    this._values.Add(id, new List<DIDAValue>());
			    }
            }
        }
	}
}