package uk.ac.reading.csmcc16;

public class FlightTripInfo {
	
	String flightID;
	String airportFrom;
	String airportTo;
	
	public FlightTripInfo(String flightID, String airportFrom, String airportTo) {
		this.flightID = flightID;
		this.airportFrom = airportFrom;
		this.airportTo = airportTo;
	}

	public String getFlightID() {
		return flightID;
	}

	public void setFlightID(String flightID) {
		this.flightID = flightID;
	}

	public String getAirportFrom() {
		return airportFrom;
	}

	public void setAirportFrom(String _airportFrom) {
		this.airportFrom = _airportFrom;
	}

	public String getAirportTo() {
		return airportTo;
	}

	public void setAirportTo(String _airportTo) {
		this.airportTo = _airportTo;
	}

}
