package uk.ac.reading.csmcc16.mapReduce;

import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class FlightPassengerMapper extends Mapper {
	
	@Override
	public void map(Object value) {
		String cols[] = ((String)value).split(",");		

		int expectedColumns = 6;

		// Syntax checking
		// (a) Check if total number of fields parsed is expected
		if (cols.length != expectedColumns) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"Total number of fields should be "+expectedColumns+", but got "+cols.length+".", (String)value);
			return;
		}

		// (b) Check if all fields are in expected format using pattern matching		
		String flightID = cols[1].trim();
		String passengerID = cols[0].trim();
		String fromAirportCode = cols[2].trim();
		String toAirportCode = cols[3].trim();

		if (!flightID.matches(Utilities.FLD_FMT_FLIGHTID)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FlightID has invalid format.", (String)value);
			return;
		}
		if (!passengerID.matches(Utilities.FLD_FMT_PASSENGERID)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"PassengerID has invalid format.", (String)value);
			return;		
		}
		if (!fromAirportCode.matches(Utilities.FLD_FMT_AIRPORTCODE)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FromAirportCode has invalid format.", (String)value);
			return;
		}
		if (!toAirportCode.matches(Utilities.FLD_FMT_AIRPORTCODE)) {		
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"ToAirportCode has invalid format.", (String)value);
			return;
		}
		if (!cols[4].matches(Utilities.FLD_FMT_DEPARTURETIME)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"DepartureTime has invalid format.", (String)value);
			return;
		}
		if (!cols[5].matches(Utilities.FLD_FMT_FLIGHTTIME)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FlightTime has invalid format.", (String)value);
			return;
		}

		Long departureTime = Long.parseLong(cols[4]);
		Long totFlightTime = Long.parseLong(cols[5]);

		
		//-------------------------------------------------------------------
		// Semantics Check
		//-------------------------------------------------------------------
		
		//(b) Check the validity of the Airport Codes with cross reference to airport data file
		Map mapRefData = (Map) this.getRefData().get("AirportInfo");
		Set setValidAirportCode = mapRefData.keySet();
		if (!setValidAirportCode.contains(fromAirportCode)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FromAirportCode is undefined in Airport Data File.", (String)value);
			return;
		}
		if (!setValidAirportCode.contains(toAirportCode)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"ToAirportCode is undefined in Airport Data File.", (String)value);
			return;
		}
		
		PassengerTripInfo valueObj = new PassengerTripInfo(passengerID, flightID, fromAirportCode, toAirportCode, departureTime, totFlightTime);
		
		emitIntermediate(flightID, valueObj);
		
	}

}
