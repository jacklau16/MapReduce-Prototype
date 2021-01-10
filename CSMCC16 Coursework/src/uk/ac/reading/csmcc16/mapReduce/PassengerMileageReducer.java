package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import uk.ac.reading.csmcc16.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class PassengerMileageReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {
		// TODO Auto-generated method stub
		double totTraveledDistance = 0.0;
//		System.out.println("[" + key + "]: " + values.size());
		
		Map mapAirportInfo = (Map) this.getRefData().get("AirportInfo");
		// Create the output object
		FlightPassengerInfo objFP = null;		
		for (int i=0; i<values.size();i++) {
			PassengerInfo objPD = (PassengerInfo)values.get(i);
			// TODO: will there be duplicated passenger?
			String passengerID = (String)key;
			String flightID = objPD.getFlightID();
			String sAirportFrom = objPD.getAirportFrom();
			String sAirportTo = objPD.getAirportTo();
			
			AirportInfo objAirportFrom = (AirportInfo) mapAirportInfo.get(sAirportFrom);
			AirportInfo objAirportTo = (AirportInfo) mapAirportInfo.get(sAirportTo);
			
			double dTraveledDistance = Utilities.getTraveledDistance(objAirportFrom.getLatitude(), objAirportFrom.getLongitude(), 
					objAirportTo.getLatitude(), objAirportTo.getLongitude());

//			System.out.println(passengerID + "," +
//					sAirportFrom + "," +
//					sAirportTo + "," +
//					dTraveledDistance
//					);
			totTraveledDistance = totTraveledDistance + dTraveledDistance;
/*			
 * 
			// Create next input list for calculating air miles
			PassengerTripInfo valObj = new PassengerTripInfo(passengerID, airportFrom, airportTo);
			
			if (mapPassengerTripInfo.containsKey(passengerID)) {
				ArrayList<Object>valArray = (ArrayList)mapPassengerTripInfo.get(passengerID);
				valArray.add(valObj);
			} else {
				ArrayList<Object>valArray = new ArrayList<Object>();
				valArray.add(valObj);				
				mapPassengerTripInfo.put(passengerID, valArray);
			}
*/	
		}
		
		this.emit(key, Double.valueOf(totTraveledDistance));
	}

}
