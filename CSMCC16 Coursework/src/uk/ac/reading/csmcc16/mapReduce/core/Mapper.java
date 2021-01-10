package uk.ac.reading.csmcc16.mapReduce.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class Mapper {
	
	File inputFile;
	
	Map<String, Object> mapKeyValuePairs = new HashMap<String, Object>();
	Map<String, Object> mapRefData;
	
    // Constructor
    public Mapper() {}

    // Execute the map function for each line of the provided file
    public void run() {
    //TODO:    for loop to iterate each line of the provided file, and call map()
		// Map Function:  key:Flight id, value: Passenger id, ...
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
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
    public abstract void map(String value);

    // Adds values to a list determined by a key
    // Map <KEY, List<VALUES>>
    // Shuffle function is performed here
    public void emitIntermediate(String key, Object value) {
		if (mapKeyValuePairs.containsKey(key)) {
			ArrayList<Object>valArray = (ArrayList)mapKeyValuePairs.get(key);
			valArray.add(value);
		} else {
			ArrayList<Object>valArray = new ArrayList<Object>();
			valArray.add(value);				
			mapKeyValuePairs.put(key, (Object)valArray);
		}
    }

	public void setFile(File file) {
		this.inputFile = file;
	}
	
	public Map getKeyValuePairs() {
		return this.mapKeyValuePairs;
	}
	
	public void setRefData(Map dataSets) {
		mapRefData = dataSets;
	}
	
	public Map getRefData() {
    	return mapRefData;
	}
}
