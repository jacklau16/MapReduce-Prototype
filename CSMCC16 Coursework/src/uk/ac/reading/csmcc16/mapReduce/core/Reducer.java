package uk.ac.reading.csmcc16.mapReduce.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Reducer {
	
	Map mapKeyValuePairs;
	Map mapResult = new HashMap();
	Map<String, Object> mapRefData;
	
    // constructor
    public Reducer() {}

    // Execute the reduce function for each key-value pair in the intermediate 
    // results output by the mapper
    public void run() {
        Iterator iterator = mapKeyValuePairs.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            reduce(entry.getKey(), entry.getValue());
        } 
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void reduce(Object key, List values);

    // Simply replace the intermediate and final result for each key
    // Map <KEY, List<VALUES>> -> Map <KEY, VALUE>
    public void emit(Object key, Object value) {
    	mapResult.put(key, value);
    }

	protected void setRecords(Map resultsFromMapper) {
		mapKeyValuePairs = resultsFromMapper;
	}
	
	public Map getResult() {
		return mapResult;
	}
	
	public void setRefData(Map dataSets) {
    	mapRefData = dataSets;
    }
	
	public Map getRefData() {
    	return mapRefData;
	}
}

