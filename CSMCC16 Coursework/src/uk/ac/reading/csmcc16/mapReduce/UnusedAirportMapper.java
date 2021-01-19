package uk.ac.reading.csmcc16.mapReduce;

import java.util.Map;
import java.util.Set;

import uk.ac.reading.csmcc16.Utilities;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class UnusedAirportMapper extends Mapper {

	@Override
	public void map(Object value) {
		String cols[] = ((String)value).split(",");		
		String dummyKey = "1"; // use dummy key for this case
		int expectedColumns = 2;

		// Syntax checking
		// (a) Check if total number of fields parsed is expected
		if (cols.length != expectedColumns) {
			Utilities.reportRowSyntaxError(getClass().getSimpleName(), this.inputFile.getName(), 
					"Total number of fields should be "+expectedColumns+".", (String)value);
			return;
		}
		
		// (b) Check if all fields are in expected format using pattern matching
		String fromAirportCode = cols[0].trim();
		String toAirportCode = cols[1].trim();
		
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
		
		emitIntermediate(dummyKey, fromAirportCode);
		emitIntermediate(dummyKey, toAirportCode);
		
	}

}
