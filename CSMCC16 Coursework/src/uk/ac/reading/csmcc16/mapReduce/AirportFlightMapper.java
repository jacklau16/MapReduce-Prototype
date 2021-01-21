package uk.ac.reading.csmcc16.mapReduce;

import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.AppFlightsFlowAnalyser;
import uk.ac.reading.csmcc16.FlightTripInfo;
import uk.ac.reading.csmcc16.Utilities;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class AirportFlightMapper extends Mapper {
	
	@Override
	public void map(Object value) {

		String cols[] = ((String)value).split(",");
		
		int expectedColumns = 6;

		//-------------------------------------------------------------------		
		// Syntax checking
		//-------------------------------------------------------------------
		// (a) Check if total number of fields parsed is expected
		if (cols.length != expectedColumns) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"Total number of fields should be "+expectedColumns+", but got "+cols.length+".", (String)value);
			return;
		}
		
		boolean bSkipRecord = false;
		
		// (b) Check if all fields are in expected format using pattern matching
		String flightID = cols[1].trim();
		String fromAirportCode = cols[2].trim();
		String toAirportCode = cols[3].trim();

		if (!flightID.matches(Utilities.FLD_FMT_FLIGHTID)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FlightID has invalid format.", (String)value);
			bSkipRecord = true;
		}
		if (!fromAirportCode.matches(Utilities.FLD_FMT_AIRPORTCODE)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FromAirportCode has invalid format.", (String)value);
			if (fromAirportCode.length()>0)
				AppFlightsFlowAnalyser.setInvalidAirportCode.add(fromAirportCode);
			bSkipRecord = true;
		}
		if (!toAirportCode.matches(Utilities.FLD_FMT_AIRPORTCODE)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"ToAirportCode has invalid format.", (String)value);
			if (toAirportCode.length()>0)
				AppFlightsFlowAnalyser.setInvalidAirportCode.add(toAirportCode);
			bSkipRecord = true;
		}
		
		if (bSkipRecord)
			return;
		
		
		//-------------------------------------------------------------------
		// Semantics Check
		//-------------------------------------------------------------------
		
		//(b) Check the validity of the Airport Codes with cross reference to airport data file
		bSkipRecord = false;
		Map mapRefData = (Map) this.getRefData().get("AirportInfo");
		Set setValidAirportCode = mapRefData.keySet();
		if (!setValidAirportCode.contains(fromAirportCode)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"FromAirportCode is undefined in Airport Data File.", (String)value);
			AppFlightsFlowAnalyser.setInvalidAirportCode.add(fromAirportCode);
			bSkipRecord = true;
		}
		if (!setValidAirportCode.contains(toAirportCode)) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"ToAirportCode is undefined in Airport Data File.", (String)value);
			AppFlightsFlowAnalyser.setInvalidAirportCode.add(toAirportCode);
			bSkipRecord = true;
		}	
		if (bSkipRecord)
			return;
		
		FlightTripInfo valueObj = new FlightTripInfo(flightID, fromAirportCode, toAirportCode);
		
		emitIntermediate(fromAirportCode, valueObj);
		
	}

}
