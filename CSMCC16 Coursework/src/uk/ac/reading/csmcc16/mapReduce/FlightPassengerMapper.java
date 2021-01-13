package uk.ac.reading.csmcc16.mapReduce;

import uk.ac.reading.csmcc16.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class FlightPassengerMapper extends Mapper {
	int rowNum=1;
	@Override
	public void map(Object value) {
		// TODO Auto-generated method stub
		String cols[] = ((String)value).split(",");
//		System.out.print(rowNum++ + ": ");
//		for (String col: cols)
//			System.out.print(col + " | ");
//		System.out.println("");
		
		String flightID = cols[1];
		String passengerID = cols[0];
		String fromAirportCode = cols[2];
		String toAirportCode = cols[3];
		Long departureTime = Long.parseLong(cols[4]);
		Long totFlightTime = Long.parseLong(cols[5]);
		
		PassengerTripInfo valueObj = new PassengerTripInfo(passengerID, flightID, fromAirportCode, toAirportCode, departureTime, totFlightTime);
		
		emitIntermediate(flightID, valueObj);
		
	}

}
