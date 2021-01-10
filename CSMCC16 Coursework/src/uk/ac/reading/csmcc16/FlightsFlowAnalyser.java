/**
 * 
 */
package uk.ac.reading.csmcc16;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import uk.ac.reading.csmcc16.mapReduce.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

/**
 * CSMCC16 Coursework
 * Description: Develop a Software Prototype of a MapReduce-like system
 * @author jacklau
 *
 */
public class FlightsFlowAnalyser {

	/**
	 * @param args
	 * args[0]: Input data file name for details of passengers flights
	 * args[1]: Input data file name of airport list
	 * args[2]: Output file name storing the "Map-Reduce" results
	 */
	public static void main(String[] args) {
		
		String className = "FlightsFlowAnalyser";
		String propFileName = "csmcc16.properties";
		Map<String, Object> dictAirportInfo = new HashMap<String, Object>();
	
		if (args.length != 3) {
			System.err.println("Error: incorrect command syntax.");
			System.err.println("Syntax: " + className + " <Passenger Data File> <Airport Data File> <Output File>");
		}
		
		// Load property values from properties file
		
		Properties configProps;
		try {
			configProps = Utilities.loadProperties(propFileName);
		} catch (Exception e) {
			System.err.println("Exception in loading properties file: " + e);			
			return;
		}
		
		
		// Load the Airport data from file
		dictAirportInfo = Utilities.loadAirportData(args[1]);
		
		// Split the input file into files with smaller partitions with defined size in properties file
		File inputFile = new File(args[0]);
		
		List<File> splitFiles = null;
		
		try {	
			splitFiles = Utilities.splitFile(inputFile, Integer.parseInt(configProps.getProperty("partition.size")));
		} catch (Exception e) {
			System.err.println("Error in splitting the input file: " + e);		
			return;
		}
		
		// Assign each split file to a mapper instance to run concurrently
		for (File sf: splitFiles) {
			System.out.println("Filename:" + sf.getName());
		}
		
		
		String[] inFiles = new String[1];
		inFiles[0] = splitFiles.get(0).getName();
		
		// Create a job for objective (a)
		Config config2 = new Config(inFiles, AirportFlightMapper.class, AirportFlightReducer.class);
		// add the airport data into the config

		Job jobAirportFlights = new Job(config2);
		try {
			jobAirportFlights.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Create a job for objective (b): List of flights
		
		Config config = new Config(inFiles, FlightPassengerMapper.class, FlightPassengerReducer.class);
		Job jobFlightPassengers = new Job(config);
		try {
			jobFlightPassengers.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		// Output for the objective (b) & (c)
        Iterator iterator = jobFlightPassengers.getResult().entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            FlightPassengerInfo objFP = (FlightPassengerInfo)entry.getValue();
            System.out.println("Flight ID: " + entry.getKey() + " (" + objFP.getNumOfPassengers() + " passengers)");
            System.out.println("From airport: " + objFP.getAirportFrom());
            System.out.println("To airport: " + objFP.getAirportTo());
            System.out.println("Departure time: " + objFP.getDepTime());
            System.out.println("Arrival time: " + objFP.getArrTime());
            System.out.println("Flight time: " + objFP.getFlightTime());
            List passengers = objFP.getPassengers();
            for (int i=0; i<passengers.size(); i++) {
            	System.out.println(passengers.get(i));
            }
            System.out.println("-------------------------------------------------------");
        } 
        
		// Output for the objective (a)
        Iterator iterator2 = jobAirportFlights.getResult().entrySet().iterator();
        PrintWriter writer = null;
		try {
			writer = new PrintWriter(new File("airports.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        while(iterator2.hasNext()) {
            Map.Entry<Object, Integer> entry = (Map.Entry) iterator2.next();
            int intFlightCount = entry.getValue().intValue();
            System.out.println("Airport: " + entry.getKey() + " (" + intFlightCount + " flights)");
            // Write to the output file
            writer.println(entry.getKey() + "," + intFlightCount);
        } 
        
        writer.close();
        
        
		// Create a job for objective (a) Unused airports
		String[] inFiles2 = new String[1];
		inFiles2[0] = "airports.csv";
		Config config3 = new Config(inFiles2, UnusedAirportMapper.class, UnusedAirportReducer.class);
		Job jobUnusedAirports = new Job(config3);
		jobUnusedAirports.addRefData("AirportInfo", ((HashMap)dictAirportInfo).clone());
		try {
			jobUnusedAirports.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Output for the objective (a) Unused airports
        Iterator iterator4 = jobUnusedAirports.getResult().entrySet().iterator();
        while(iterator4.hasNext()) {
            Map.Entry<Object, Set> entry = (Map.Entry) iterator4.next();
            Iterator airportNames = entry.getValue().iterator();
            System.out.println("Unused airports:");
            while(airportNames.hasNext()) {
            	System.out.println(airportNames.next());
            }
            System.out.println("-------------------------------------------------------");
        } 
		
		// Create a job for objective(d) Passenger having earned the highest air miles
		Config config4 = new Config(inFiles, PassengerMileageMapper.class, PassengerMileageReducer.class);
		Job jobPassengerMileage = new Job(config4);
		jobPassengerMileage.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		try {
			jobPassengerMileage.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Output for the objective (d)
		
		//Sort the passenger records by their total mileage in descending order 
        Set<Entry<String, Double>> set = jobPassengerMileage.getResult().entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        for (Entry<String, Double> entry : list) {
            System.out.println(entry.getKey() + ": " + entry.getValue());

        }
        System.out.println("_____________________");
        
		// Wait for all mappers to complete their tasks
		
		// Assign to reducers
		
		
	}


}
