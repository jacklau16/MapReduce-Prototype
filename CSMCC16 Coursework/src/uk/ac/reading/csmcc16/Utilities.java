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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Utilities {


	public static double getTraveledDistance(double latitudeFrom, double longitudeFrom, double latitudeTo, double longitudeTo) {
		// Calculate the mileage of the trip using Haversine formula, then convert to nautical miles
		// References: 
		// (i) https://en.wikipedia.org/wiki/Haversine_formula
		// (ii) https://www.movable-type.co.uk/scripts/latlong.html
		
		// Haversine formula:	a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
		//						c = 2 ⋅ atan2( √a, √(1−a) )
		//						d = R ⋅ c
		// where	φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km);
		// note that angles need to be in radians to pass to trig functions!
		
		double R = 6371.0;
		double phi_1 = Math.toRadians(latitudeFrom);
		double phi_2 = Math.toRadians(latitudeTo);
		double phi_diff = phi_2 - phi_1;
		double lambda_diff = Math.toRadians(longitudeTo - longitudeFrom);
		double a = Math.pow(Math.sin(phi_diff/2), 2) + Math.cos(phi_1) * Math.cos(phi_2) * Math.pow(Math.sin(lambda_diff/2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//double c = 2 * Math.asin(Math.sqrt(a));
		double d = R * c;
		
		// 1 nautical mile = 1.852 km
		return d / 1.852;
	}
	
	
	public static List<File> splitFile(File file, int sizeOfFileInBytes) throws IOException {
	    int counter = 1;
	    List<File> files = new ArrayList<File>();
	    int sizeOfChunk = sizeOfFileInBytes;
	    String eof = System.lineSeparator();
	    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	        String name = file.getName();
	        String line = br.readLine();
	        while (line != null) {
	            File newFile = new File(file.getParent(), name + "."
	                    + String.format("%03d", counter++));
	            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
	                int fileSize = 0;
	                while (line != null) {
	                    byte[] bytes = (line + eof).getBytes(Charset.defaultCharset());
	                    if (fileSize + bytes.length > sizeOfChunk)
	                        break;
	                    out.write(bytes);
	                    fileSize += bytes.length;
	                    line = br.readLine();
	                }
	            }
	            files.add(newFile);
	        }
	    }
	    return files;
	}
	
	
	public static Properties loadProperties(String propFileName) throws IOException {
		
		InputStream inputStream = null;
		Properties prop = null;
		 
		try {
			prop = new Properties();
 
			//inputStream = FlightsFlowAnalyser.class.getResourceAsStream(propFileName);
			inputStream = new FileInputStream(propFileName);
 
			if (inputStream != null)
				prop.load(inputStream);
 
		} catch (Exception e) {
			throw e;//System.err.println("Exception: " + e);
		} finally {
			if (inputStream != null)
				inputStream.close();
		}
		return prop;
	}
	

	public static Map<String, Object> loadAirportData(String file) {
		// Read the Airport Data File
		Map<String, Object> dictAirportInfo = new HashMap<String, Object>(); 
		System.out.println("Loading airport data from '" + file + "'...");
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
				String cols[] = line.split(",");
				double aLat, aLong;
				String airportCode;
				try {
					airportCode = cols[1];
					aLat = Double.parseDouble(cols[2]);
					aLong = Double.parseDouble(cols[3]);
					
					AirportInfo airportInfo = new AirportInfo(cols[0], cols[1], aLat, aLong);
					dictAirportInfo.put(airportCode, airportInfo);
					// Add every airport code into the set first
//					usedAirports.add(airportCode);
				} catch (NumberFormatException e) {
					// skip this record
					// TODO add error logging
				} catch (Exception e) {
					// skip this record
					System.err.println("Error: record skipped.");
				}
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		return dictAirportInfo;
	}
}
