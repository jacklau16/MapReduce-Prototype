package uk.ac.reading.csmcc16.mapReduce.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import uk.ac.reading.csmcc16.Logger;

public abstract class Mapper implements Runnable {
	
	protected File inputFile;
	boolean skipHeader = false;
	
//	static ConcurrentHashMap<String, Object> mapKeyValuePairs = new ConcurrentHashMap<String, Object>();
	ConcurrentHashMap<Object, Object> mapKeyValuePairs;
	Map<String, Object> mapRefData;
	
    // Constructor
    public Mapper() {}

    // Execute the map function for each line of the provided file
    public void run() {
    //TODO:    for loop to iterate each line of the provided file, and call map()
		// Map Function:  key:Flight id, value: Passenger id, ...
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			if (skipHeader) { // skip the column header if needed
				br.readLine();
				Logger.getInstance().logDebug(getClass().getSimpleName()+": skipped header row.");
			}
		    String line;
		    while ((line = br.readLine()) != null) {
		    	map(line);
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    // Abstract map function to be overwritten by objective-specific class
    public abstract void map(Object value);

    // Adds values to a list determined by a key
    // Map <KEY, List<VALUES>>
    // Shuffle function is performed here
    public void emitIntermediate(Object key, Object value) {
		if (mapKeyValuePairs.containsKey(key)) {
			CopyOnWriteArrayList<Object>valArray = (CopyOnWriteArrayList)mapKeyValuePairs.get(key);
			valArray.add(value);
		} else {
			CopyOnWriteArrayList<Object>valArray = new CopyOnWriteArrayList<Object>();
			valArray.add(value);				
			mapKeyValuePairs.put(key, (Object)valArray);
		}
    }

	public void setFile(File file) {
		this.inputFile = file;
	}
	
	public void setSkipHeader(boolean bSkipHeader) {
		this.skipHeader = bSkipHeader;
	}
	
	public ConcurrentHashMap getKeyValuePairs() {
		return this.mapKeyValuePairs;
	}
	
	public void setRefData(Map dataSets) {
		mapRefData = dataSets;
	}
	
	public void setKeyValuePairs(ConcurrentHashMap mapKeyValuePairs) {
		this.mapKeyValuePairs = mapKeyValuePairs;
	}
	
	public Map getRefData() {
    	return mapRefData;
	}
}
