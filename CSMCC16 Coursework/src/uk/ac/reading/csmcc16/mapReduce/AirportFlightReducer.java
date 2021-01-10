package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.FlightTripInfo;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class AirportFlightReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {

		Set<String> setFlights = new HashSet<String>();
		
		for (int i=0; i<values.size();i++) {
			FlightTripInfo objFTI = (FlightTripInfo)values.get(i);
			String flightID = objFTI.getFlightID();
			setFlights.add(flightID);
			this.emit("UsedAirports", objFTI, objFTI);
		}	
		this.emit("FlightCount", key, (Object)Integer.valueOf(setFlights.size()));
	}

}
