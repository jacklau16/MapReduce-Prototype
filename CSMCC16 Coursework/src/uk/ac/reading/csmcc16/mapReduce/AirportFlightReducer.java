package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.mapReduce.core.*;

public class AirportFlightReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {

		Set<String> setFlights = new HashSet<String>();
		
		for (int i=0; i<values.size();i++) {
			String flightID = (String)values.get(i);
			setFlights.add(flightID);
		}	

		this.emit(key, (Object)Integer.valueOf(setFlights.size()));
	}

}
