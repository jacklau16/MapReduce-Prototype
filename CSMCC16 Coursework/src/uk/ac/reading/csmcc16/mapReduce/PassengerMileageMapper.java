package uk.ac.reading.csmcc16.mapReduce;

import uk.ac.reading.csmcc16.PassengerInfo;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class PassengerMileageMapper extends Mapper {
	int rowNum=1;
	@Override
	public void map(String value) {
		// TODO Auto-generated method stub
		String cols[] = value.split(",");
		System.out.print(rowNum++ + ": ");
		for (String col: cols)
			System.out.print(col + " | ");
		System.out.println("");
		
		String passengerID = cols[0];
		String flightID = cols[1];
		String fromAirportCode = cols[2];
		String toAirportCode = cols[3];
		Long departureTime = Long.parseLong(cols[4]);
		Long totFlightTime = Long.parseLong(cols[5]);
		
		PassengerInfo valueObj = new PassengerInfo(passengerID, flightID, fromAirportCode, toAirportCode);
		
		emitIntermediate(passengerID, valueObj);
		
	}

}