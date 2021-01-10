package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import uk.ac.reading.csmcc16.mapReduce.core.*;

public class AirportFlightReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {
		// TODO Auto-generated method stub
		int flightCount = 0;
		System.out.println("[" + key + "]: " + values.size());
		// Create the output object
		String airportCode = (String)key;
		List lstFlights = new ArrayList();
		
		for (int i=0; i<values.size();i++) {
			String flightID = (String)values.get(i);
			// ignore duplicated flight numbers
			if (!lstFlights.contains(flightID)) {
				flightCount++;
				lstFlights.add(flightID);
			}
		}
		
/*			
 * 
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
*/	
		
		this.emit(key, (Object)Integer.valueOf(flightCount));
	}

}
