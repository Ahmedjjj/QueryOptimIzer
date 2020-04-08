package qp.utils;

import java.util.*;

public class IndexBuilder {

    HashMap<Object, ArrayList<Tuple>> indexMap;
    int index;

    public IndexBuilder(){
        indexMap = new HashMap<>();
    }

    public ArrayList<Tuple> get(Object Data){
        return indexMap.get(Data);
    }

    public void setIndex(int currentIndex){
        index = currentIndex;
    }

    public void createIndex(Batch batch){
        fillIndexMap(batch.tuples);
    }

    public void fillIndexMap(ArrayList<Tuple> currRecords){
        for(Tuple tuple : currRecords){
            add(tuple.dataAt(index), tuple);
        }
    }

    public void add(Object key, Tuple data){
        ArrayList<Tuple> tupleSet = indexMap.get(key);
        /** Check whether tupleSet is empty  **/
        if(tupleSet == null ||tupleSet.isEmpty()){
            tupleSet = new ArrayList<Tuple>();
            indexMap.put(key, tupleSet);
        }
        tupleSet.add(data);
        indexMap.put(key, tupleSet);
    }


}
