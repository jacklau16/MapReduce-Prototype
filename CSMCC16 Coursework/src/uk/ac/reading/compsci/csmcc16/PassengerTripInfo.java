package uk.ac.reading.compsci.csmcc16;

public class PassengerTripInfo {
	
	String _passengerID;
	String _airportFrom;
	String _airportTo;
	
	public PassengerTripInfo(String passengerID, String airportFrom, String airportTo) {
		this._passengerID = passengerID;
		this._airportFrom = airportFrom;
		this._airportTo = airportTo;
	}

	public String getPassengerID() {
		return _passengerID;
	}

	public void setPassengerID(String _passengerID) {
		this._passengerID = _passengerID;
	}

	public String getAirportFrom() {
		return _airportFrom;
	}

	public void setAirportFrom(String _airportFrom) {
		this._airportFrom = _airportFrom;
	}

	public String getAirportTo() {
		return _airportTo;
	}

	public void setAirportTo(String _airportTo) {
		this._airportTo = _airportTo;
	}

}
