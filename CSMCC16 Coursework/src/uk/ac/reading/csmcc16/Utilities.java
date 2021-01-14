package uk.ac.reading.csmcc16;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.TableView;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.text.Text;

public class Utilities {

	public static double getTraveledDistance(double latitudeFrom, double longitudeFrom, double latitudeTo, double longitudeTo) {
		// Calculate the mileage of the trip using Haversine formula, then convert to nautical miles
		// References: 
		// (i) https://en.wikipedia.org/wiki/Haversine_formula
		// (ii) https://www.movable-type.co.uk/scripts/latlong.html
		
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
 
			//inputStream = FlightsFlowAnalyser.class.getResourceAsStream(propFileName);
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
		System.out.println("Loading airport data from '" + file + "'...");
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
				String cols[] = line.split(",");
				double aLat, aLong;
				String airportCode;
				try {
					airportCode = cols[1];
					aLat = Double.parseDouble(cols[2]);
					aLong = Double.parseDouble(cols[3]);
					
					AirportInfo airportInfo = new AirportInfo(cols[0], cols[1], aLat, aLong);
					dictAirportInfo.put(airportCode, airportInfo);
					// Add every airport code into the set first
//					usedAirports.add(airportCode);
				} catch (NumberFormatException e) {
					// skip this record
					// TODO add error logging
				} catch (Exception e) {
					// skip this record
					//System.err.println("Load Airport Data: Error in parsing a record, it is skipped.");
					Logger.getInstance().logError("Load Airport Data: Error in parsing a record, it is skipped.");
				}
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	public static void setCursorWait(final Scene scene)
	{
	    Runnable r=new Runnable() {

	        @Override
	        public void run() {
	             scene.setCursor(Cursor.WAIT);
	        }
	    };
	    Thread t=new Thread(r);
	    t.start();
	}
	
	public static void setCursorDefault(final Scene scene)
	{
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
