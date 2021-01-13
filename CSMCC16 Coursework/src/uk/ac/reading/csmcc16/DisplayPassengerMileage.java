package uk.ac.reading.csmcc16;

public class DisplayPassengerMileage {
	String passengerID;
	Double totMileage;
	String flightID;
	String airportFromCode;
	String airportFromName;
	String airportToCode;
	String airportToName;
	Double flightMileage;
	
	public String getPassengerID() {
		return passengerID;
	}
	public void setPassengerID(String passengerID) {
		this.passengerID = passengerID;
	}
	public Double getTotMileage() {
		return totMileage;
	}
	public void setTotMileage(Double totMileage) {
		this.totMileage = totMileage;
	}
	public String getFlightID() {
		return flightID;
	}
	public void setFlightID(String flightID) {
		this.flightID = flightID;
	}
	public String getAirportFromCode() {
		return airportFromCode;
	}
	public void setAirportFromCode(String airportFromCode) {
		this.airportFromCode = airportFromCode;
	}
	public String getAirportToCode() {
		return airportToCode;
	}
	public void setAirportToCode(String airportToCode) {
		this.airportToCode = airportToCode;
	}
	public String getAirportFromName() {
		return airportFromName;
	}
	public void setAirportFromName(String airportFromName) {
		this.airportFromName = airportFromName;
	}
	public String getAirportToName() {
		return airportToName;
	}
	public void setAirportToName(String airportToName) {
		this.airportToName = airportToName;
	}
	public Double getFlightMileage() {
		return flightMileage;
	}
	public void setFlightMileage(Double flightMileage) {
		this.flightMileage = flightMileage;
	}
}
