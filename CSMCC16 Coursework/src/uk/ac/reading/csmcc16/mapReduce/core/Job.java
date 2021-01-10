package uk.ac.reading.csmcc16.mapReduce.core;

import java.awt.List;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

//TODO: Job class should be generic

public class Job {
    // Job configuration
    private Config config;

    // Global object to store intermediate and final results
    // TODO: Move the map object from main to here
    protected  ConcurrentHashMap mapKeyValuePairs = new ConcurrentHashMap();
    protected  ConcurrentHashMap mapJobResult = new ConcurrentHashMap();
    protected Map mapRefData = new HashMap();

    // Constructor
    public Job(Config config) {
        this.config = config; 
    }

    // Run the job given the provided configuration
    public void run() throws Exception {
        // Initialise the map to store intermediate results
        //map = new Map();
        // Execute the map and reduce phases in sequence
        
        //TODO: pass the properties values to here
        
        //TODO: move file splitter to here
        //TODO: Multi-threading in mapper
        map();
        
 //       combine();
        
        reduce();
    }

    // Map each provided file using an instance of the mapper specified by the job 
    // configuration
    private void map() throws Exception {
    	int i = 1;
    	Mapper mapper = null;
    	ArrayList<Thread> threadList = new ArrayList<Thread>();
        for(File file : config.getFiles()) {
            mapper = config.getMapperInstance(file);
            mapper.setRefData(mapRefData);
            mapper.setKeyValuePairs(mapKeyValuePairs);
            Thread thread = new Thread(mapper, "Thread " + i++);
            thread.start();
            threadList.add(thread);
 //           mapper.run();         
        }
        System.out.println(mapper.getClass() + ": " + (i-1) + " threads created");
        
        // Wait for all threads to complete
        Iterator it = threadList.iterator();
        while (it.hasNext()) {
        	Thread thread = (Thread) it.next();
        	thread.join();
        }
        mapKeyValuePairs = mapper.getKeyValuePairs();   
        System.out.println(mapper.getClass() + ": all threads completed");
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
         //   reducer = config.getReducerInstance(mapJobResult));
            reducer.setRefData(mapRefData);
            reducer.setResult(mapJobResult);
            Thread thread = new Thread(reducer, "Thread " + i++);
            thread.start();
            threadList.add(thread);
            //reducer.run();          
  //          reduce(entry.getKey(), entry.getValue());
        } 
        
        System.out.println(reducer.getClass() + ": " + (i-1) + " threads created");
        
        // Wait for all threads to complete
        Iterator it = threadList.iterator();
        while (it.hasNext()) {
        	Thread thread = (Thread) it.next();
        	thread.join();
        }
        System.out.println(reducer.getClass() + ": all threads completed");
//        mapJobResult = reducer.getResult();
    }
    
    public Map getResult() {
    	return mapJobResult;
    }
    
    public void addRefData(String key, Object values) {  	
    	mapRefData.put(key, values);
    }
}
