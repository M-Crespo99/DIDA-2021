namespace DIDAStorage.Exceptions{
    public class NoSuchVersionException : DIDAStorageException{

        private readonly string _id;
        private readonly DIDAVersion _version;
        public NoSuchVersionException(string id, DIDAVersion version){
            this._id = id;
            this._version = version;
        }

        public override string ToString()
        {
            return "No DIDARecord found with id: " + this._id + " and "
            + "Version: " + this._version;
        }

        public string ID => this._id;
        public DIDAVersion Version => this._version;
    }
}