package uk.ac.reading.csmcc16.mapReduce;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import uk.ac.reading.csmcc16.FlightPassengerInfo;
import uk.ac.reading.csmcc16.PassengerTripInfo;
import uk.ac.reading.csmcc16.mapReduce.core.*;

public class FlightPassengerReducer extends Reducer {

	@Override
	public void reduce(Object key, List values) {
		// TODO Auto-generated method stub
		int passengerCount = 0;
//		System.out.println("[" + key + "]: " + values.size());
		// Create the output object
		FlightPassengerInfo objFP = null;		
		for (int i=0; i<values.size();i++) {
			PassengerTripInfo objPD = (PassengerTripInfo)values.get(i);
			// TODO: will there be duplicated passenger?
			passengerCount++;
			String flightID = (String)key;
			String passengerID = objPD.getPassengerID();
			String airportFrom = objPD.getAirportFrom();
			String airportTo = objPD.getAirportTo();
			String depTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(objPD.getDepTime()*1000));
			String arrTime =  new SimpleDateFormat("hh:mm:ss").format(new Date(objPD.getDepTime()*1000+objPD.getTotFlightTime()*60*1000));
			String flightTime = new SimpleDateFormat("hh:mm:ss").format(new Date(objPD.getTotFlightTime()*60*1000));
//			System.out.println(passengerID + "," +
//					airportFrom + "," +
//					airportTo + "," +
//					depTime + "," +
//					arrTime
//					);
			if (i==0)
				 objFP = new FlightPassengerInfo(flightID, airportFrom, airportTo, depTime, arrTime, flightTime);	
			objFP.addPassenger(passengerID);
		}
		
		this.emit(key, objFP);
	}

}
