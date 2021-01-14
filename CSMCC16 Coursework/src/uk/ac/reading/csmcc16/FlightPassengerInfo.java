package uk.ac.reading.csmcc16;

import java.util.ArrayList;
import java.util.List;

public class FlightPassengerInfo {
	
	String _flightID;
	List _passengerID = new ArrayList();
	String _airportFrom;
	String _airportTo;
	String _depTime;
	String _arrTime;
	String _flightTime;
	double _flightMileage;
	
	public String getFlightID() {
		return _flightID;
	}

	public void setFlightID(String flightID) {
		this._flightID = flightID;
	}

	public List getPassengers() {
		return _passengerID;
	}
	
	public int getNumOfPassengers() {
		return _passengerID.size();
	}

	public void addPassenger(String passengerID) {
		this._passengerID.add(passengerID);
	}

	public boolean containsPassenger(String passengerID) {
		return this._passengerID.contains(passengerID);
	}
	
	public String getAirportFrom() {
		return _airportFrom;
	}

	public void setAirportFrom(String airportFrom) {
		this._airportFrom = airportFrom;
	}

	public String getAirportTo() {
		return _airportTo;
	}

	public void setAirportTo(String airportTo) {
		this._airportTo = airportTo;
	}

	public String getDepTime() {
		return _depTime;
	}

	public void setDepTime(String depTime) {
		this._depTime = depTime;
	}

	public String getArrTime() {
		return _arrTime;
	}

	public void setArrTime(String arrTime) {
		this._arrTime = arrTime;
	}
	
	public String getFlightTime() {
		return _flightTime;
	}

	public void setFlightTime(String flightTime) {
		this._flightTime = flightTime;
	}
	
	public double getFlightMileage() {
		return _flightMileage;
	}

	public void setFlightTime(double flightMileage) {
		this._flightMileage = flightMileage;
	}
	
	public FlightPassengerInfo(String flightID, String airportFrom, String airportTo, String depTime, String arrTime, String flightTime, double flightMileage) {
		this._flightID = flightID;
		this._airportFrom = airportFrom;
		this._airportTo = airportTo;
		this._depTime = depTime;
		this._arrTime = arrTime;
		this._flightTime = flightTime;
		this._flightMileage = flightMileage;
	}
}
