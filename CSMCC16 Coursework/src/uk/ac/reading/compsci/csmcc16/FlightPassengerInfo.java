package uk.ac.reading.compsci.csmcc16;

public class FlightPassengerInfo {
	
	String _flightID;
	String _passengerID;
	String _airportFrom;
	String _airportTo;
	long _depTime;
	long _totFlightTime;
	
	public String getFlightID() {
		return _flightID;
	}

	public void setFlightID(String flightID) {
		this._flightID = flightID;
	}

	public String getPassengerID() {
		return _passengerID;
	}

	public void setPassengerID(String passengerID) {
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

	
	public FlightPassengerInfo(String flightID, String passengerID, String airportFrom, String airportTo, long depTime, long totFlightTime) {
		this._flightID = flightID;
		this._passengerID = passengerID;
		this._airportFrom = airportFrom;
		this._airportTo = airportTo;
		this._depTime = depTime;
		this._totFlightTime = totFlightTime;
	}
}
