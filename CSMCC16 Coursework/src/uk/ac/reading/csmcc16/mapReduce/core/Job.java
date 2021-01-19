package uk.ac.reading.csmcc16.mapReduce.core;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import uk.ac.reading.csmcc16.Logger;

//TODO: Job class should be generic

public class Job {
    // Job configuration
    private Config config;

    // Global object to store intermediate and final results
    protected  ConcurrentHashMap mapKeyValuePairs = new ConcurrentHashMap();
    protected  ConcurrentHashMap<String, Object> mapJobResults = new ConcurrentHashMap();
    
    protected Map mapRefData = new HashMap();

    // Constructor
    public Job(Config config) {
        this.config = config; 
        mapJobResults.put("default", new ConcurrentHashMap());
    }

    // Run the job given the provided configuration
    public void run() throws Exception {

        map();
        
        reduce();
    }

    // Map each provided file using an instance of the mapper specified by the job configuration
    private void map() throws Exception {
    	int i = 1;
    	Mapper mapper = null;
    	ArrayList<Thread> threadList = new ArrayList<Thread>();
        for(File file : config.getFiles()) {
            mapper = config.getMapperInstance(file);
            mapper.setRefData(mapRefData);
            mapper.setKeyValuePairs(mapKeyValuePairs);
            if(i==1) // For the first file partition, skip column header if needed
            	mapper.setSkipHeader(config.getSkipHeader());
            Thread thread = new Thread(mapper, "Thread " + i++);
            thread.start();
            threadList.add(thread);      
        }
        
        Logger.getInstance().logMessage(mapper.getClass().getSimpleName() + ": " + (i-1) + (i>2?" threads created":" thread created"));
        
        // Wait for all threads to complete
        Iterator it = threadList.iterator();
        while (it.hasNext()) {
        	Thread thread = (Thread) it.next();
        	thread.join();
        }
        mapKeyValuePairs = mapper.getKeyValuePairs();   
        Logger.getInstance().logMessage(mapper.getClass().getSimpleName() + ": all threads completed");
    }

    // Reduce the intermediate results output by the map phase using an instance of 
    // the reducer specified by the job configuration
    private void reduce() throws Exception {
    	int i = 1;
        Reducer reducer = null;
        
       	ArrayList<Thread> threadList = new ArrayList<Thread>();
       	
        Iterator iterator = mapKeyValuePairs.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, CopyOnWriteArrayList<Object>> entry = (Entry<Object, CopyOnWriteArrayList<Object>>) iterator.next();
            
            reducer = config.getReducerInstance(entry.getKey(), entry.getValue());
            reducer.setRefData(mapRefData);
            reducer.setResult(mapJobResults);
            Thread thread = new Thread(reducer, "Thread " + i++);
            thread.start();
            threadList.add(thread);
        } 
        Logger.getInstance().logMessage(reducer.getClass().getSimpleName() + ": " + (i-1) + (i>2?" threads created":" thread created"));
        
        // Wait for all threads to complete
        Iterator it = threadList.iterator();
        while (it.hasNext()) {
        	Thread thread = (Thread) it.next();
        	thread.join();
        }
        Logger.getInstance().logMessage(reducer.getClass().getSimpleName() + ": all threads completed");
    }
    
    public Map getJobResult() {
    	return (Map)mapJobResults.get("default");
    }
    
    public Map getJobResult(String key) {
    	return (Map)mapJobResults.get(key);
    }
    
    public void addRefData(String key, Object values) {  	
    	mapRefData.put(key, values);
    }
    
    public void addJobResultBucket(String key) {	
        mapJobResults.put(key, new ConcurrentHashMap());
    }
}
