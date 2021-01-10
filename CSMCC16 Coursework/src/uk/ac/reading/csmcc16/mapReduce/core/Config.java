package uk.ac.reading.csmcc16.mapReduce.core;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * MapReduce Job Configuration
 * Stores the file specifications provided at run-time and
 * uses reflection to set objective-specific mapper and reducer classes.
 *
 * Areas for improvement:
 * - Output to file or implement a user interface to display results
 */
public class Config {
    // Input files to process
    private File[] files;

    // Classes to implement job-specific map and reduce functions
    private Class mapper, reducer;

    // Constructor
    public Config(List<String> inFiles, Class mapper, Class reducer) {
        init(inFiles);
        this.mapper = mapper;
        this.reducer = reducer;
    }

    // Initialise a job using the provided arguments
    private void init(List<String> inFiles) {
  //      if(inFiles == null || inFiles.length == 0) {
  //          System.out.println("Usage: java MapReduce <files>\n\tProcess a set of files listed by <files> using a trivial MapReduce implementation.");
  //          System.exit(1);
  //      }
        this.files = new File[inFiles.size()];
        for(int i=0; i<inFiles.size(); i++)
            this.files[i] = new File(inFiles.get(i));
    }

    // Generic file reader returning an iterator cycling through each line of the specified file
    protected static Iterator read(File file) throws IOException {
        List record = new ArrayList();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while((line = br.readLine()) != null)
            record.add(line);
        br.close();
        return record.iterator();
    }

    // Return the list of files to process
    protected File[] getFiles() {
        return this.files;
    }

    // Using reflection get an instance of the mapper operating on a specified file
    protected Mapper getMapperInstance(File file) throws Exception {
        Mapper mapper = (Mapper) this.mapper.getConstructor().newInstance();
        mapper.setFile(file);
        return mapper;
    }

    // Using reflection get an instance of the reducer operating on a chunk of the intermediate results
    protected Reducer getReducerInstance(Object key, CopyOnWriteArrayList results) throws Exception {
        Reducer reducer = (Reducer) this.reducer.getConstructor().newInstance();
        reducer.setKey(key);
        reducer.setRecords(results);
        return reducer;
    }
}
