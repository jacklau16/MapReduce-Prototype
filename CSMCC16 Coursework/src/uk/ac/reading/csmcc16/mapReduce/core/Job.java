package uk.ac.reading.csmcc16.mapReduce.core;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//TODO: Job class should be generic

public class Job {
    // Job configuration
    private Config config;

    // Global object to store intermediate and final results
    // TODO: Move the map object from main to here
    protected  Map mapKeyValuePairs;
    protected  Map mapJobResult;
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
        for(File file : config.getFiles()) {
            Mapper mapper = config.getMapperInstance(file);
            mapper.setRefData(mapRefData);
            mapper.run();
            mapKeyValuePairs = mapper.getKeyValuePairs();
            
        }
    }

    // Reduce the intermediate results output by the map phase using an instance of 
    // the reducer specified by the job configuration
    private void reduce() throws Exception {
        Reducer reducer = config.getReducerInstance(mapKeyValuePairs);
        reducer.setRefData(mapRefData);
        reducer.run();
        mapJobResult = reducer.getResult();
    }
    
    public Map getResult() {
    	return mapJobResult;
    }
    
    public void addRefData(String key, Object values) {
    	
    	mapRefData.put(key, values);
    }
}
