namespace DIDAStorage.Exceptions{
    public class NoSuchRecordException : DIDAStorageException{

        private readonly string _id;
        public NoSuchRecordException(string id){
            this._id = id;
        }

        public override string ToString()
        {
            return "No DIDARecord found with id: " + this._id;
        }

        public string ID => this._id;
    }
}