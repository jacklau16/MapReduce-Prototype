package uk.ac.reading.csmcc16.mapReduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import uk.ac.reading.csmcc16.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class PassengerMileageReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {

		double totTraveledDistance = 0.0;
		
		Map mapAirportInfo = (Map) this.getRefData().get("AirportInfo");
		// Create the output object
		FlightPassengerInfo objFP = null;
		
		Set<String> setFlights = new HashSet<String>();
		List<PassengerTripInfo>lstPTI = new <PassengerTripInfo>CopyOnWriteArrayList();
		
		for (int i=0; i<values.size();i++) {
			PassengerTripInfo objPD = (PassengerTripInfo)values.get(i);
			// TODO: will there be duplicated passenger?
			String passengerID = (String)key;
			String flightID = objPD.getFlightID();
			String sAirportFrom = objPD.getAirportFrom();
			String sAirportTo = objPD.getAirportTo();
			
			//-------------------------------------------------------------------
			// Semantics Check
			//-------------------------------------------------------------------	
			//(a) Use a Set object to store the flight IDs of a passenger to ensure 
			//    uniqueness and correct count
			if (setFlights.add(flightID)) {
				AirportInfo objAirportFrom = (AirportInfo) mapAirportInfo.get(sAirportFrom);
				AirportInfo objAirportTo = (AirportInfo) mapAirportInfo.get(sAirportTo);
			
				double dTraveledDistance = Utilities.calculateTraveledDistance(objAirportFrom.getLatitude(), objAirportFrom.getLongitude(), 
					objAirportTo.getLatitude(), objAirportTo.getLongitude());

				objPD.setFlightMileage(dTraveledDistance);		
				lstPTI.add(objPD);
				totTraveledDistance = totTraveledDistance + dTraveledDistance;
			} else {
				// Duplicated FlightID
				Logger.getInstance().logWarning(getClass().getSimpleName()+": Duplicated FlightID '"+flightID+"', record skipped.");
				Utilities.setErrorStatus(true);
			}
		}
		
//		this.emit("FlightMileage", key, values);
		this.emit("FlightMileage", key, lstPTI);
		this.emit("PassengerMileage", key, Double.valueOf(totTraveledDistance));
	}

}
