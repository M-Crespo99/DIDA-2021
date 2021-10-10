using System;

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
	}
}
