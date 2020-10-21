package uk.ac.reading.compsci.csmcc16;

public class AirportInfo {
	String _name;
	String _code;
	double _latitude;
	double _longitude;
	
	public String getName() {
		return _name;
	}

	public void setName(String _name) {
		this._name = _name;
	}

	public String getCode() {
		return _code;
	}

	public void setCode(String _code) {
		this._code = _code;
	}

	public double getLatitude() {
		return _latitude;
	}

	public void setLatitude(double _latitude) {
		this._latitude = _latitude;
	}

	public double getLongitude() {
		return _longitude;
	}

	public void setLongitude(double _longitude) {
		this._longitude = _longitude;
	}


	public AirportInfo(String name, String code, double latitude, double longitude) {
		this._name = name;
		this._code = code;
		this._latitude = latitude;
		this._longitude = longitude;
	}
}
