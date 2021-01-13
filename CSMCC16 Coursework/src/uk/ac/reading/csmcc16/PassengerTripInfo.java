package uk.ac.reading.csmcc16;

public class PassengerTripInfo {
	
	String _flightID;
	String _passengerID;
	String _airportFrom;
	String _airportTo;
	long _depTime;
	long _totFlightTime;
	double _flightMileage;
	
	public String getFlightID() {
		return _flightID;
	}

	public void setFlightID(String flightID) {
		this._flightID = flightID;
	}

	public String getPassengerID() {
		return _passengerID;
	}
	
	public void getPassengerID(String passengerID) {
		this._passengerID = passengerID;
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

	public long getDepTime() {
		return _depTime;
	}

	public void setDepTime(long depTime) {
		this._depTime = depTime;
	}

	public long getTotFlightTime() {
		return _totFlightTime;
	}

	public void setTotFlightTime(long totFlightTime) {
		this._totFlightTime = totFlightTime;
	}

	public double getFlightMileage() {
		return _flightMileage;
	}
	
	public void setFlightMileage(double flightMileage) {
		this._flightMileage = flightMileage;
	}
	
	public PassengerTripInfo(String passengerID, String flightID, String airportFrom, String airportTo, long depTime, long totFlightTime) {
		this._passengerID = passengerID;
		this._flightID = flightID;
		this._airportFrom = airportFrom;
		this._airportTo = airportTo;
		this._depTime = depTime;
		this._totFlightTime = totFlightTime;
	}
	
	public PassengerTripInfo(String passengerID, String flightID, String airportFrom, String airportTo) {
		this._passengerID = passengerID;
		this._flightID = flightID;
		this._airportFrom = airportFrom;
		this._airportTo = airportTo;
	}
}
