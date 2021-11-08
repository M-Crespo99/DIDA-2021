﻿using System.Collections.Generic;

namespace GossipLib
{
    public class LamportClock
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
            return str + " ]";
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

        public bool isConcurrent(LamportClock otherClock){
            return (!(this >= otherClock) && !(this <= otherClock)); 
        }
    }
}