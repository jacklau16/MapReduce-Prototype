package uk.ac.reading.csmcc16.mapReduce.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class Reducer implements Runnable {
	
	Object oKey;
	CopyOnWriteArrayList<Object> lstValues;
	ConcurrentHashMap<String, Object> mapResult;// = new ConcurrentHashMap();
	Map<String, Object> mapRefData;
	
    // constructor
    public Reducer() {}

    // Execute the reduce function for each key-value pair in the intermediate 
    // results output by the mapper
    public void run() {
//        Iterator iterator = lstValues.entrySet().iterator();
//        while(iterator.hasNext()) {
//            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
//            reduce(entry.getKey(), entry.getValue());
//        } 
    	reduce(oKey, lstValues);
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void reduce(Object key, List<Object> values);

    // Simply replace the intermediate and final result for each key
    // Map <KEY, List<VALUES>> -> Map <KEY, VALUE>
    public void emit(Object key, Object value) {
    	emit("default", key, value);
    }
    
    public void emit(String outputName, Object key, Object value) {
    	((ConcurrentHashMap<Object, Object>) mapResult.get(outputName)).put(key, value);
    }

	protected void setRecords(CopyOnWriteArrayList<Object> resultsFromMapper) {
		lstValues = resultsFromMapper;
	}
	
	protected void setKey(Object key) {
		oKey = key;
	}
	
	public Map getResult() {
		return mapResult;
	}
	
	public void setRefData(Map dataSets) {
    	mapRefData = dataSets;
    }
	
	public void setResult(ConcurrentHashMap<String, Object> mapResult) {
		this.mapResult = mapResult;
	}
	
	public Map getRefData() {
    	return mapRefData;
	}
	
}

