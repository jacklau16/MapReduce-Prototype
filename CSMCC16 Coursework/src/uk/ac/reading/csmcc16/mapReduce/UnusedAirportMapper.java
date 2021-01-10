package uk.ac.reading.csmcc16.mapReduce;

import uk.ac.reading.csmcc16.mapReduce.core.*;

public class UnusedAirportMapper extends Mapper {

	@Override
	public void map(Object value) {
		// TODO Auto-generated method stub
		String cols[] = ((String)value).split(",");		
		String airportCode = cols[0];
		String dummyKey = "1"; // use dummy key for this case
		
//		Integer numOfFlights = Integer.valueOf(cols[1]);
				
		emitIntermediate(dummyKey, airportCode);
		
	}

}
