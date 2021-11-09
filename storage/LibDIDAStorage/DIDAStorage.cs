namespace DIDAStorage {
	public interface IDIDAStorage {
		DIDARecord Read(string id, DIDAVersion version);
		DIDAVersion Write(string id, string val, StorageFrontend.LamportClock clock);
		DIDAVersion UpdateIfValueIs(string id, string oldvalue, string newvalue);
	}
	public struct DIDARecord {
		public string id;
		public DIDAVersion version;

		public StorageFrontend.LamportClock valueTS;
		public string val;
	}


	public struct DIDAVersion {
		public int versionNumber;
		public int replicaId;

		public StorageFrontend.LamportClock replicaTS;



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

		public override string ToString(){
			return string.Format("Version Number: {0}\nReplica ID: {1}\nReplica TS: {2}\n", versionNumber, replicaId, replicaTS.ToString());
		}
	}

	public struct DIDAValue {
		public DIDAVersion version;
		public StorageFrontend.LamportClock valueTS;
		public string value;

		public override string ToString(){
			return string.Format("Value: {0}\nValue Timestamp: {1}\nVersion: {2}", value, valueTS, version);
		}

	}
}
