package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import uk.ac.reading.csmcc16.AirportInfo;
import uk.ac.reading.csmcc16.FlightPassengerInfo;
import uk.ac.reading.csmcc16.PassengerTripInfo;
import uk.ac.reading.csmcc16.Utilities;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class FlightPassengerReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {

		// Create the output object
		FlightPassengerInfo objFP = null;	
		Map mapAirportInfo = (Map) this.getRefData().get("AirportInfo");
		double dTraveledDistance = 0.0;
		for (int i=0; i<values.size();i++) {
			PassengerTripInfo objPD = (PassengerTripInfo)values.get(i);
			String flightID = (String)key;
			String passengerID = objPD.getPassengerID();
			String airportFrom = objPD.getAirportFrom();
			String airportTo = objPD.getAirportTo();
			String depTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(objPD.getDepTime()*1000));
			String arrTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(objPD.getDepTime()*1000+objPD.getTotFlightTime()*60*1000));
			String flightTime = new SimpleDateFormat("h:mm:ss").format(new Date(objPD.getTotFlightTime()*60*1000));
			AirportInfo objAirportFrom = (AirportInfo) mapAirportInfo.get(airportFrom);
			AirportInfo objAirportTo = (AirportInfo) mapAirportInfo.get(airportTo);
			
			dTraveledDistance = Utilities.calculateTraveledDistance(objAirportFrom.getLatitude(), objAirportFrom.getLongitude(), 
					objAirportTo.getLatitude(), objAirportTo.getLongitude());
			if (i==0)
				 objFP = new FlightPassengerInfo(flightID, airportFrom, airportTo, depTime, arrTime, flightTime, dTraveledDistance);	

			//-------------------------------------------------------------------
			// Semantics Check
			//-------------------------------------------------------------------	
			//(a) Utilising the Set object inside the FlightPassengerInfo class
			//    to store the Passenger IDs of a flight to ensure uniqueness and correct count			
			objFP.addPassenger(passengerID);			
		}
		
		this.emit(key, objFP);
	}

}
