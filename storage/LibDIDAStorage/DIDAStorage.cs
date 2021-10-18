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

		public override string ToString(){
			return string.Format("Value: {0}\nVersion: {1}", value, version);
		}

	}
}
