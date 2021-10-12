using System;
using System.Collections.Generic;

namespace DIDAStorage {
	public interface IDIDAStorage {
		DIDARecord Read(string id, DIDAVersion version);
		DIDAVersion Write(string id, string val);
		DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue);
	}
	public struct DIDARecord {
		public string id;
		public DIDAVersion version;
		public string val;
	}


	public struct DIDAVersion {
		public int versionNumber;
		public int replicaId;

		public static bool operator ==(DIDAVersion v1, DIDAVersion v2){
			return (v1.versionNumber == v2.versionNumber) && (v1.replicaId == v2.replicaId);
		}

		public static bool operator !=(DIDAVersion v1, DIDAVersion v2){
			return (v1.versionNumber != v2.versionNumber) || (v1.replicaId != v2.replicaId);
		}

		public static bool operator <(DIDAVersion v1, DIDAVersion v2){
			return (v1.versionNumber < v2.versionNumber) || 
			((v1.versionNumber == v2.versionNumber) &&(v1.replicaId < v2.replicaId));
		}
		public static bool operator >(DIDAVersion v1, DIDAVersion v2){
			return (v1.versionNumber > v2.versionNumber) || 
			((v1.versionNumber == v2.versionNumber) && (v1.replicaId > v2.replicaId));
		}

		public override bool Equals(object obj)
		{
			//
			// See the full list of guidelines at
			//   http://go.microsoft.com/fwlink/?LinkID=85237
			// and also the guidance for operator== at
			//   http://go.microsoft.com/fwlink/?LinkId=85238
			//
			
			if (obj == null || GetType() != obj.GetType())
			{
				return false;
			}
			
			return base.Equals (obj);
		}
		
		// override object.GetHashCode
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}

	public struct DIDAValue {
		public DIDAVersion version;

		public string value;
		
	}

	

	public class DIDAStorage : IDIDAStorage {
		private int MAX_VERSIONS = 10;

		private int replicaId = 0;
		private Dictionary<string, DIDAValue[]> values = new Dictionary<string, DIDAValue[]>();

		public DIDAStorage(int replicaId){
			this.replicaId = replicaId;
		}

		public DIDARecord Read(string id, DIDAVersion version){
			if(values.ContainsKey(id)){
				lock(this.values){
					//TODO: Maybe do something else other than cycle through all of the values
					foreach(DIDAValue v in this.values[id]){
						if(v.version == version){
							return new DIDARecord{
								id = id,
								version = v.version,
								val = v.value
							};
						}
					}
				}
				//TODO: Throw Exception
				return new DIDARecord();
			}
			else{
				//TODO: Throw exception
				return new DIDARecord();
			}
		}

		public DIDAVersion Write(string id, string val){
			DIDAValue valueToWrite = new DIDAValue();
		
			valueToWrite.value = val;

			DIDAVersion newVersion = new DIDAVersion{
				replicaId = this.replicaId
			};

			var currentValues = values[id];

			int oldestIndex = FindIndexOfOldestVersion(currentValues);

			//Increment the version
			newVersion.versionNumber = FindMostRecentVersion(currentValues) + 1;

			valueToWrite.version = newVersion;

			//Write on top of the oldest	
			if(currentValues.Length == MAX_VERSIONS){
				values[id][oldestIndex] = valueToWrite;

			}else{
				values[id][values[id].Length] = valueToWrite;
			}

			return newVersion;
		}
		
		public DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue){
			throw new NotImplementedException();
		}

		private int FindMostRecentVersion(DIDAValue[] values){
			DIDAVersion newestVersion = values[0].version;

			foreach(DIDAValue v in values){
				if(v.version > newestVersion){
					newestVersion = v.version;
				}
			}
			return newestVersion.versionNumber; 
		}
		private int FindIndexOfOldestVersion(DIDAValue[] values){
			int indexOfOldest = 0;

			DIDAVersion oldestVersion = values[0].version;

			foreach(DIDAValue v in values){
				if(v.version < oldestVersion){
					indexOfOldest = Array.IndexOf(values, v);
				}
			}
			return indexOfOldest; 
		}
	}
}
