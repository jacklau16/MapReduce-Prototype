/**
 * 
 */
package uk.ac.reading.compsci.csmcc16;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author jacklau
 *
 */
public class ACompPassengerData {

	/**
	 * @param args
	 * args[0]: Input data file name for details of passengers flights
	 * args[1]: Input data file name of airport list
	 * args[2]: Output file name storing the "Map-Reduce" results
	 */
	public static void main(String[] args) {
	
		if (args.length != 3) {
			System.err.println("Error: incorrect command syntax.");
			System.err.println("Syntax: " + ACompPassengerData.class.getName() + " <Passenger Data File> <Airport Data File> <Output File>");
		}
		
		ArrayList<Object> listFlightPassengerInfo = new ArrayList<Object>();
		Map<String, Object> mapFlightPassengerInfo = new HashMap<String, Object>();
		Map<String, Object> dictAirportInfo = new HashMap<String, Object>();
		Set<String> usedAirports = new HashSet<String>();
		Map<String, Object> mapPassengerTripInfo = new HashMap<String, Object>();
		
		// Map Function:  key:Flight id, value: Passenger id, ...
		try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
		    String line;
		    while ((line = br.readLine()) != null) {
				String cols[] = line.split(",");
//				for (String col: cols)
//					System.out.print(col + " | ");
//				System.out.println("");
				//String key = cols[1]; // Flight id
				FlightPassengerInfo valueObj = new FlightPassengerInfo(cols[1], cols[0], cols[2], cols[3], Long.parseLong(cols[4]), Long.parseLong(cols[5]));
				//map1.put(key, values);
				listFlightPassengerInfo.add(valueObj);
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Sort Function:
		for (Object obj: listFlightPassengerInfo) {
			FlightPassengerInfo valObj = (FlightPassengerInfo)obj;
			if (mapFlightPassengerInfo.containsKey(valObj.getFlightID())) {
				ArrayList<Object>valArray = (ArrayList)mapFlightPassengerInfo.get(valObj.getFlightID());
				valArray.add(valObj);
			} else {
				ArrayList<Object>valArray = new ArrayList<Object>();
				valArray.add(valObj);				
				mapFlightPassengerInfo.put(valObj.getFlightID(), valArray);
			}
		}
		
		// Reduce Function: 
		for (String key : mapFlightPassengerInfo.keySet()) {
			ArrayList a = (ArrayList) mapFlightPassengerInfo.get(key);
			int passengerCount = 0;
			System.out.println("[" + key + "]: " + a.size());
			for (int i=0; i<a.size();i++) {
				FlightPassengerInfo fpInfo = (FlightPassengerInfo)a.get(i);
				passengerCount++;
				// TODO calculate the miles traveled by the passenger
				String passengerID = fpInfo.getPassengerID();
				String airportFrom = fpInfo.getAirportFrom();
				String airportTo = fpInfo.getAirportTo();
				String depTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(fpInfo.getDepTime()*1000));
				String arrTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(fpInfo.getDepTime()*1000+fpInfo.getTotFlightTime()*60*1000));
				System.out.println(passengerID + "," +
						airportFrom + "," +
						airportTo + "," +
						depTime + "," +
						arrTime
						);
				
				// Create next input list for calculating air miles
				PassengerTripInfo valObj = new PassengerTripInfo(passengerID, airportFrom, airportTo);
				
				if (mapPassengerTripInfo.containsKey(passengerID)) {
					ArrayList<Object>valArray = (ArrayList)mapPassengerTripInfo.get(passengerID);
					valArray.add(valObj);
				} else {
					ArrayList<Object>valArray = new ArrayList<Object>();
					valArray.add(valObj);				
					mapPassengerTripInfo.put(passengerID, valArray);
				}
		
			}
		}

		// Read the Airport Data File
		try (BufferedReader br = new BufferedReader(new FileReader(args[1]))) {
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
					usedAirports.add(airportCode);
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

		// Reduce Function 2: 
		String maxMilesPassenger = "";
		double maxMiles = 0.0;

		for (String key : mapPassengerTripInfo.keySet()) {
			double totNauticalMilage = 0.0;
			ArrayList a = (ArrayList) mapPassengerTripInfo.get(key);			
			//System.out.println("[" + key + "]: " + a.size());
			for (int i=0; i<a.size();i++) {
				PassengerTripInfo ptInfo = (PassengerTripInfo)a.get(i);
				AirportInfo airportInfoFrom = (AirportInfo)(dictAirportInfo.get(ptInfo.getAirportFrom()));
				AirportInfo airportInfoTo = (AirportInfo)(dictAirportInfo.get(ptInfo.getAirportTo()));
				
				double airportFromLat = airportInfoFrom.getLatitude();
				double airportFromLong = airportInfoFrom.getLongitude();
				double airportToLat = airportInfoTo.getLatitude();
				double airportToLong = airportInfoTo.getLongitude();
				
				double nauticalMilage = ACompPassengerData.getTraveledDistance(airportFromLat, airportFromLong, airportToLat, airportToLong);

				totNauticalMilage += nauticalMilage;

				System.out.println(key + "," +
						airportInfoFrom.getCode() + "," +
						airportInfoTo.getCode() + "," +
						nauticalMilage
						);
			}
			if (totNauticalMilage > maxMiles) {
				maxMilesPassenger = key;
				maxMiles = totNauticalMilage;
			}
		}
		
		System.out.println("Highest air mileage: " + maxMilesPassenger + " (" +	maxMiles + " miles)");
		
		// Data Validation using RegEx
//		if(cols[2].matches("^[\\w\\-]+$") && cols[4].matches("^\\d{1,9}$")) {
//			country.set(cols[2]);
//			stats.set(Integer.parseInt(cols[4]), 1);
//			context.write(country, stats);
//		}
		
	}

	public static double getTraveledDistance(double lat1, double long1, double lat2, double long2) {
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
		double phi_1 = Math.toRadians(lat1);
		double phi_2 = Math.toRadians(lat2);
		double phi_diff = phi_2 - phi_1;
		double lambda_diff = Math.toRadians(long2 - long1);
		double a = Math.pow(Math.sin(phi_diff/2), 2) + Math.cos(phi_1) * Math.cos(phi_2) * Math.pow(Math.sin(lambda_diff/2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//double c = 2 * Math.asin(Math.sqrt(a));
		double d = R * c;
		
		// 1 nautical mile = 1.852 km
		return d / 1.852;
	}
}
