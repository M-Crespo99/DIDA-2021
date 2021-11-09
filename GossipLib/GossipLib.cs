using System.Collections.Generic;
using System;

namespace GossipLib
{
    public enum operationType{
        READ = 1,
        WRITE = 2,
        UPDATE_IF_VALUE_IS = 3,
    }
    public struct operation{
        public string key;
        public operationType opType;

        public string  newValue;

        public int versionNumber;

        public override string ToString()
        {
            return String.Format("< K: {0} V: {1} VN: {2} >", key, newValue,versionNumber);
        }

    }
    public class GossipLogRecord{
        public int _replicaId;

        public LamportClock _updateTS;

        public LamportClock _prev;

        public LamportClock _replicaTS;

        public int _operationIdentifier;

        public operation _operation;


        public GossipLogRecord(int replicaId,
                                LamportClock updateTS,
                                LamportClock prev,
                                LamportClock replicaTS,
                                int operationIdentifier,
                                operation op){
            

            this._replicaId = replicaId;
            this._prev = prev;
            this._updateTS = updateTS;
            this._operationIdentifier = operationIdentifier;
            this._operation = op;
            this._replicaTS = replicaTS;
        }

        public override string ToString()
        {
            return String.Format("LogRecord: <{0}, {1}, {2}, {3}, {4}> {5}\n", this._replicaId,
                                                                                _updateTS.ToString(),
                                                                                _operation,
                                                                                _prev.ToString(),
                                                                                _operationIdentifier,
                                                                                _replicaTS.ToString());
        }

    }
    public class LamportClock : IComparable<LamportClock>
    {
        private int _numberOfReplicas {get;}
        private int[] _clock {get;}

        public LamportClock(int numberOfReplicas){
            this._clock = new int[numberOfReplicas];
            this._numberOfReplicas = numberOfReplicas;
            for(int i = 0; i < numberOfReplicas; i++){
                this._clock[i] = 0;
            }
        }

        public LamportClock(List<int> list){
            this._clock = new int[list.Count];
            this._numberOfReplicas = list.Count;
            for(int i = 0; i < this._numberOfReplicas; i++){
                this._clock[i] = list[i];
            }
        }

        public LamportClock DeepCopy(){
            var copy = new LamportClock(this._numberOfReplicas);


            for(int i = 0; i < this._numberOfReplicas; i++){
                copy.assign(i, this.At(i));
            }

            return copy;
        }

        public LamportClock(int[] clock){
            this._clock = clock;
            this._numberOfReplicas = clock.Length;
        }


        public void merge(LamportClock otherClock){
            for(int i = 0; i < this._numberOfReplicas; i++){
                if(otherClock.At(i) > this._clock[i]){
                    this._clock[i] = otherClock.At(i);
                }
                else if(otherClock.At(i) < this._clock[i]){
                    otherClock.assign(i, this._clock[i]);
                }
            }
        }

        public int CompareTo(LamportClock that){
            if(this < that){
                return 0;
            }else if(this > that){
                return 1;
            }
            return 1;
        }
        public int assign(int position, int newValue){
            if(position > this._numberOfReplicas || position < 0){
                return -1; 
            }
            this._clock[position] = newValue;
            return newValue;
        }


        public int incrementAt(int position){
            if(position > this._numberOfReplicas || position < 0){
                return -1; 
            }

            this._clock[position] = this._clock[position] + 1;

            return this._clock[position];
        }

        public override string ToString()
        {
            string str = "[ ";

            foreach(int i in this._clock){
                str += i.ToString() + " ";
            }
            return str + "]";
        }

        public int At(int i){
            if(i < 0 || i > this._numberOfReplicas){
                return -1;
            }
            return this._clock[i];
        }

        public List<int> toList(){
            var listToReturn = new List<int>();

            foreach(int i in this._clock){
                listToReturn.Add(i);
            }
            return listToReturn;
        }

        public static bool operator >=(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) < l2.At(i))){
                    return false;
                }
            }
            return true;
        }
        public static bool operator <=(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) > l2.At(i))){
                    return false;
                }
            }
            return true;
        }

        public static bool operator <(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) > l2.At(i))){
                    return false;
                }
            }
            return true;
        }
        public static bool operator >(LamportClock l1, LamportClock l2) {
            for(int i = 0; i < l1._numberOfReplicas; i++){
                if((l2.At(i) != -1) && (l1.At(i) < l2.At(i))){
                    return false;
                }
            }
            return true;
        }

        public bool isConcurrent(LamportClock otherClock){
            return (!(this >= otherClock) && !(this <= otherClock)); 
        }
    }
}
