package uk.ac.reading.csmcc16;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.TableView;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.text.Text;

public class Utilities {

	// Regular expressions for checking the format correctness of each input fields
	public static final String FLD_FMT_AIRPORTNAME = "^[A-Z ]{3,20}$";
	public static final String FLD_FMT_AIRPORTCODE = "^[A-Z]{3}$";
	public static final String FLD_FMT_LATITUDE = "^-?\\d{3,13}$|^-?(?=\\d+\\.\\d+$).{4,14}$";
	public static final String FLD_FMT_LONGITUDE = "^-?\\d{3,13}$|^-?(?=\\d+\\.\\d+$).{4,14}$";
	public static final String FLD_FMT_PASSENGERID = "^[A-Z]{3}\\d{4}[A-Z]{2}\\d$";
	public static final String FLD_FMT_FLIGHTID = "^[A-Z]{3}\\d{4}[A-Z]$";
	public static final String FLD_FMT_DEPARTURETIME = "^\\d{10}$";
	public static final String FLD_FMT_FLIGHTTIME = "^\\d{1,4}$";	
	
	private static boolean bErrorStatus = false;
	
	public static void setErrorStatus(boolean bStatus) {
		bErrorStatus = bStatus;
	}

	public static void reportRowSyntaxError(String className, String fileName, String errMsg, String colEntry) {
		Logger.getInstance().logError(className+": "+errMsg+"\n"+
				"Input record skipped in file '"+fileName+"': " + colEntry);
		setErrorStatus(true);
	}
	
	public static boolean getErrorStatus() {
		return bErrorStatus;
	}

	// Calculate the mileage of the trip using Haversine formula, then convert to nautical miles
	// References: 
	// (i) https://en.wikipedia.org/wiki/Haversine_formula
	// (ii) https://www.movable-type.co.uk/scripts/latlong.html
	public static double calculateTraveledDistance(double latitudeFrom, double longitudeFrom, double latitudeTo, double longitudeTo) {		
		// Haversine formula:	a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
		//						c = 2 ⋅ atan2( √a, √(1−a) )
		//						d = R ⋅ c
		// where	φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km);
		// note that angles need to be in radians to pass to trig functions!
		
		double R = 6371.0;
		double phi_1 = Math.toRadians(latitudeFrom);
		double phi_2 = Math.toRadians(latitudeTo);
		double phi_diff = phi_2 - phi_1;
		double lambda_diff = Math.toRadians(longitudeTo - longitudeFrom);
		double a = Math.pow(Math.sin(phi_diff/2), 2) + Math.cos(phi_1) * Math.cos(phi_2) * Math.pow(Math.sin(lambda_diff/2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//double c = 2 * Math.asin(Math.sqrt(a));
		double d = R * c;
		
		// 1 nautical mile = 1.852 km
		return d / 1.852;
	}
	
	// Routine for calculating Levenshtein Distance of two strings
	public static int calculateLevenshteinDistance(String a, String b) {
		a = a.toLowerCase();
		b = b.toLowerCase();
		// i == 0
		int [] costs = new int [b.length() + 1];
		for (int j = 0; j < costs.length; j++)
			costs[j] = j;
		for (int i = 1; i <= a.length(); i++) {
			// j == 0; nw = lev(i - 1, j)
			costs[0] = i;
			int nw = i - 1;
			for (int j = 1; j <= b.length(); j++) {
				int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
				nw = costs[j];
				costs[j] = cj;
			}
		}
		return costs[b.length()];
	}

	public static List getSuggestedAirportCodes(String input) {
		Map<String, Integer> mapResult = new <String, Integer>HashMap();
		Iterator iter = AppFlightsFlowAnalyser.dictAirportInfo.keySet().iterator();
		int minDistance = 9999;
		
		while (iter.hasNext()) {
			String sValidAirportCode = (String)iter.next();
			int levenshteinDistance = calculateLevenshteinDistance(input, sValidAirportCode);
			mapResult.put(sValidAirportCode, Integer.valueOf(levenshteinDistance));
			if (levenshteinDistance < minDistance)
				minDistance = levenshteinDistance;
		}
		
		Iterator iter2 = mapResult.entrySet().iterator();
		List<String> lstResult = new<String> ArrayList();
		while (iter2.hasNext()) {
			Map.Entry<Object, Integer> entry = (Map.Entry) iter2.next();
			String key = (String) entry.getKey();
			if (entry.getValue().intValue()==minDistance)
				lstResult.add(key);
		}
		return lstResult;
	}
	
	public static List<File> splitFile(File file, int sizeOfFileInBytes) throws IOException {
	    int counter = 1;
	    List<File> files = new ArrayList<File>();
	    int sizeOfChunk = sizeOfFileInBytes;
	    String eof = System.lineSeparator();
	    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	        String name = file.getName();
	        String line = br.readLine();
	        while (line != null) {
	            File newFile = new File(file.getParent(), name + "."
	                    + String.format("%03d", counter++));
	            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
	                int fileSize = 0;
	                while (line != null) {
	                    byte[] bytes = (line + eof).getBytes(Charset.defaultCharset());
	                    if (fileSize + bytes.length > sizeOfChunk)
	                        break;
	                    out.write(bytes);
	                    fileSize += bytes.length;
	                    line = br.readLine();
	                }
	            }
	            files.add(newFile);
	        }
	    }
	    return files;
	}
	
	
	public static Properties loadProperties(String propFileName) throws IOException {
		
		InputStream inputStream = null;
		Properties prop = null;
		 
		try {
			prop = new Properties();
 
			inputStream = new FileInputStream(propFileName);
 
			if (inputStream != null)
				prop.load(inputStream);
 
		} catch (Exception e) {
			throw e;//System.err.println("Exception: " + e);
		} finally {
			if (inputStream != null)
				inputStream.close();
		}
		return prop;
	}
	

	public static Map<String, Object> loadAirportData(String file) {
		// Read the Airport Data File
		Map<String, Object> dictAirportInfo = new HashMap<String, Object>(); 
		Logger.getInstance().logMessage("Loading airport data from '" + file + "'...");
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
				String cols[] = line.split(",");
				int expectedColumns = 4;

				// Syntax checking
				// (a) Check if total number of fields parsed is expected
				if (cols.length != expectedColumns) {
					Utilities.reportRowSyntaxError("Utilities.loadAirportData", file, 
							"Total number of fields should be "+expectedColumns+", but got "+cols.length+".", line);
					continue;
				}
				double aLat, aLong;
				String airportCode;
				try {
					airportCode = cols[1].trim();
					
					if (!airportCode.matches(Utilities.FLD_FMT_AIRPORTCODE)) {
						Utilities.reportRowSyntaxError("Utilities.loadAirportData", file, 
								"AirportCode has invalid format.", line);
						continue;
					}
					if (!cols[2].trim().matches(Utilities.FLD_FMT_LATITUDE)) {
						Utilities.reportRowSyntaxError("Utilities.loadAirportData", file, 
								"Latitude has invalid format.", line);
						continue;
					}
					if (!cols[3].trim().matches(Utilities.FLD_FMT_LONGITUDE)) {
						Utilities.reportRowSyntaxError("Utilities.loadAirportData", file, 
								"Longitude has invalid format.", line);
						continue;
					}
					aLat = Double.parseDouble(cols[2]);
					aLong = Double.parseDouble(cols[3]);
					
					AirportInfo airportInfo = new AirportInfo(cols[0], cols[1], aLat, aLong);
					dictAirportInfo.put(airportCode, airportInfo);
				} catch (Exception e) {
					// skip this record
					Utilities.reportRowSyntaxError("Utilities.loadAirportData", file, 
							e.getMessage(), line);
					//Logger.getInstance().logError("Load Airport Data: Error in parsing a record, it is skipped.");
				}
		    }
		} catch (Exception e) {
			Logger.getInstance().logError("Utilities.loadAirportData: " + e.getMessage());
			setErrorStatus(true);
		}		
		
		return dictAirportInfo;
	}


	public static void autoResizeTableViewColumns( TableView<?> table )
	{
	    //Set the right policy
	    table.setColumnResizePolicy( TableView.UNCONSTRAINED_RESIZE_POLICY);
	    table.getColumns().stream().forEach( (column) ->
	    {
	        //Minimal width = columnheader
	        Text t = new Text( column.getText() );
	        double max = t.getLayoutBounds().getWidth();
	        for ( int i = 0; i < table.getItems().size(); i++ )
	        {
	            //cell must not be empty
	            if ( column.getCellData( i ) != null )
	            {
	                t = new Text( column.getCellData( i ).toString() );
	                double calcwidth = t.getLayoutBounds().getWidth();
	                //remember new max-width
	                if ( calcwidth > max )
	                {
	                    max = calcwidth;
	                }
	            }
	        }
	        //set the new max-width with some extra space
	        column.setPrefWidth( max + 10.0d );
	    } );
	}


	public static void autoResizeTreeTableViewColumns( TreeTableView<?> table ) {
		//Set the right policy
		table.setColumnResizePolicy( TreeTableView.UNCONSTRAINED_RESIZE_POLICY);

		ArrayList lstTblCols = new ArrayList();

		for (int i=0; i<table.getColumns().size();i++) {
			lstTblCols.add(table.getColumns().get(i));
			for (int j=0; j<table.getColumns().get(i).getColumns().size(); j++) {
				lstTblCols.add(table.getColumns().get(i).getColumns().get(j));
			}    	
		}

		for (int k=0; k<lstTblCols.size(); k++) {
			//Minimal width = columnheader
			TreeTableColumn column = (TreeTableColumn) lstTblCols.get(k);
			Text t = new Text( column.getText() );
			double max = t.getLayoutBounds().getWidth();
			ArrayList<TreeItem> lstTreeItems = new ArrayList<TreeItem>();

			for(TreeItem<?> child: table.getRoot().getChildren()){
				lstTreeItems.add(child);
				if(!child.getChildren().isEmpty()){
					for(TreeItem<?> grandChild: child.getChildren()) {
						lstTreeItems.add(grandChild);
						if(!grandChild.getChildren().isEmpty()){
							for(TreeItem<?> grandGrandChild: grandChild.getChildren())
								lstTreeItems.add(grandGrandChild);
						}
					} 
				} 
			}

			for ( int i = 0; i < lstTreeItems.size(); i++ )
			{
				//cell must not be empty
				if ( column.getCellData( i ) != null )
				{
					t = new Text( column.getCellData( i ).toString() );
					double calcwidth = t.getLayoutBounds().getWidth();
					//remember new max-width
					if ( calcwidth > max )
					{
						max = calcwidth;
					}
				}
			}
			//set the new max-width with some extra space
			column.setPrefWidth( max + 10.0d );
		}
	}
	
	public static void setCursorWait(final Scene scene) {
	    Runnable r=new Runnable() {

	        @Override
	        public void run() {
	             scene.setCursor(Cursor.WAIT);
	        }
	    };
	    Thread t=new Thread(r);
	    t.start();
	}
	
	public static void setCursorDefault(final Scene scene) {
	        Runnable r=new Runnable() {

	        @Override
	        public void run() {
	             scene.setCursor(Cursor.DEFAULT);
	        }
	    };
	    Thread t=new Thread(r);
	    t.start();
	}
}
