package uk.ac.reading.csmcc16.mapReduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.mapReduce.core.*;

public class UnusedAirportReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {
		// TODO Auto-generated method stub

		Map mapRefData = (Map) this.getRefData().get("AirportInfo");
		Set setUnusedAirports = mapRefData.keySet();
		for (int i=0; i<values.size();i++) {
			String airportCode = (String)values.get(i);
			setUnusedAirports.remove(airportCode);

		}		
		this.emit(key, setUnusedAirports);
	}

}
