/**
 * 
 */
package uk.ac.reading.csmcc16;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.Control;
import javafx.scene.control.Separator;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TabPane.TabClosingPolicy;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.control.cell.TreeItemPropertyValueFactory;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import uk.ac.reading.csmcc16.mapReduce.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

/**
 * CSMCC16 Coursework
 * Description: Develop a Software Prototype of a MapReduce-like system
 * @author jacklau
 *
 */
public class AppFlightsFlowAnalyser extends Application {

	static Properties configProps;
	static String propFileName = "csmcc16.properties";
	public static Map<String, Object> dictAirportInfo = new HashMap<String, Object>();
	public static Set<String> setInvalidAirportCode = ConcurrentHashMap.newKeySet();
	
	// JavaFX objects
	TextField txtFldAirportDataFile;
	TextField txtFldPassengerDataFile;
	static TableView tblVwAirportFlights;
	static TableView tblVwUnusedAirports;
	static TreeTableView trTblVwFlightPassengers;
	static TreeTableView trTblVwPassengerMileage;
	TabPane tbPnMain;
	static Stage stgPrimaryStage;

	
	// Starting point of the application
	public static void main(String[] args) {
	
		System.out.println("AppFlightsFlowAnalyser");
		System.out.println("java.runtime.version: " + System.getProperty("java.runtime.version", "(undefined)"));
		System.out.println("javafx.version: " + System.getProperty("javafx.version", "(undefined)"));
		
		// Load the system properties from file
		try {
			configProps = Utilities.loadProperties(propFileName);
		} catch (Exception e) {
			System.err.println("Exception in loading properties file: " + e);			
			return;
		}
		
		// Set the logger debug mode and verbose mode based on the properties file
		Logger.getInstance().setDebugMode(Boolean.getBoolean(configProps.getProperty("log.debug.msg", "false")));
		Logger.getInstance().setVerboseMode(Boolean.getBoolean(configProps.getProperty("log.verbose.msg", "false")));		
		
		
		// JavaFX will then call start(Stage) in this class		
		launch(args);
	
	}

	// JavaFX GUI starting pointing
	@Override
	public void start(Stage primaryStage) {
		try {						
			stgPrimaryStage = primaryStage;

			// Create a TabPane control and add all tab pages to it
			tbPnMain = new TabPane();
			tbPnMain.setTabClosingPolicy(TabClosingPolicy.UNAVAILABLE);
			tbPnMain.getTabs().add(new Tab("Start", createStartVBox()));
			tbPnMain.getTabs().add(new Tab("Airports Analysis", createAirportsAnalysisGridPane()));
			tbPnMain.getTabs().add(new Tab("Flights Analysis", createFlightsAnalysisVBox()));
			tbPnMain.getTabs().add(new Tab("Passengers Analysis", createPassengersAnalysisVBox()));
			tbPnMain.getTabs().add(new Tab("Log Messages", createLoggingVBox()));

			primaryStage.setScene(new Scene(new VBox(tbPnMain), 1000, 600));
			primaryStage.setTitle("Flights Flow Analyser (MapReduce Prototype)");
			primaryStage.show();
			
		} catch(Exception e) {
			System.err.println("Exception in starting the JavaFX GUI: " + e);	
		}
	}
	
	public VBox createStartVBox() {
		
	    Image imgChecked = new Image(getClass().getClassLoader().getResourceAsStream("green_check.png"));
	    ImageView imgVwGreenCheck1 = new ImageView(imgChecked);
	    imgVwGreenCheck1.setFitHeight(30);
	    imgVwGreenCheck1.setFitWidth(30);
	    imgVwGreenCheck1.setVisible(false);
	    
	    ImageView imgVwGreenCheck2 = new ImageView(imgChecked);
	    imgVwGreenCheck2.setFitHeight(30);
	    imgVwGreenCheck2.setFitWidth(30);
	    imgVwGreenCheck2.setVisible(false);
	    
		FileChooser fileChooser = new FileChooser();
	    TextField textField1 = new TextField();
	    textField1.setMinWidth(380);
	    textField1.setEditable(false);
	    textField1.setStyle("-fx-font-size: 16; ");
	    TextField textField2 = new TextField();
	    textField2.setMinWidth(380);
	    textField2.setEditable(false);
	    textField2.setStyle("-fx-font-size: 16; ");
	    Separator separator1 = new Separator(Orientation.HORIZONTAL);
	    Separator separator2 = new Separator(Orientation.HORIZONTAL);
	    
		Button button1 = new Button("1. Select Airport Data File");
		button1.setStyle("-fx-font-size: 20; ");
		button1.setPrefWidth(420);

		Button button2 = new Button("2. Select Passenger Data File");
		button2.setStyle("-fx-font-size: 20; ");
		button2.setPrefWidth(420);
		
		Button button3 = new Button("3. Perform Data Analysis");
		button3.setStyle("-fx-font-size: 20; ");
		button3.setPrefWidth(420);
		button3.setDisable(true);
		
		// Placeholder for data file names with full path
	    TextField txtFldAirportDataFile = new TextField();
	    TextField txtFldPassengerDataFile = new TextField();
	    
		button1.setOnAction(e -> {
			File selectedFile = fileChooser.showOpenDialog(stgPrimaryStage);
			if (selectedFile != null) {
				textField1.setText(selectedFile.getName());
				txtFldAirportDataFile.setText(selectedFile.getAbsolutePath());
			    imgVwGreenCheck1.setVisible(true);
				if (textField1.getText().isBlank() || textField2.getText().isBlank()) {
					button3.setDisable(true);
				} else {
					button3.setDisable(false);
				}
			}
		});
		

		button2.setOnAction(e -> {
			File selectedFile = fileChooser.showOpenDialog(stgPrimaryStage);
			if (selectedFile != null) {
				textField2.setText(selectedFile.getName());
				txtFldPassengerDataFile.setText(selectedFile.getAbsolutePath());
			    imgVwGreenCheck2.setVisible(true);
				if (textField1.getText().isBlank() || textField2.getText().isBlank()) {
					button3.setDisable(true);
				} else {
					button3.setDisable(false);
				}
			}
		});

		button3.setOnAction(e -> {
			if (textField1.getText().isBlank() || textField2.getText().isBlank()) {
				Alert a = new Alert(AlertType.ERROR);
				a.setContentText("Please select the files for both Step 1 and Step 2.");
	            a.show();
			} else {
		
				// Clear the result if any
				tblVwAirportFlights.getItems().clear();
				tblVwUnusedAirports.getItems().clear();
				trTblVwFlightPassengers.setRoot(null);
				trTblVwPassengerMileage.setRoot(null);
				
				// Kick-off MapReduce Jobs
				startMapReduceJobs(txtFldAirportDataFile.getText(), txtFldPassengerDataFile.getText());
				
				// Switch to 1st result tab page
				tbPnMain.getSelectionModel().select(1);
			}
		});

		HBox hbox1 = new HBox(10, textField1, imgVwGreenCheck1);
		hbox1.setAlignment(Pos.CENTER);
		HBox hbox2 = new HBox(10, textField2, imgVwGreenCheck2);
		hbox2.setAlignment(Pos.CENTER);
		
		VBox vbox3 = new VBox(20, button1, hbox1, separator1, button2, hbox2, separator2, button3);
		vbox3.setAlignment(Pos.CENTER);
		
		return vbox3;
		
	}
	
	public GridPane createAirportsAnalysisGridPane() {

		tblVwAirportFlights = new TableView();
		tblVwUnusedAirports = new TableView();
	
		// Set the table view to resize columns automatically
		tblVwAirportFlights.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
		tblVwUnusedAirports.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
		
		// Use Map to add data		
		TableColumn<Map, String> column_a1 = new TableColumn<>("Airport Code");
		column_a1.setCellValueFactory(new MapValueFactory<>("airportCode"));
		column_a1.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a2 = new TableColumn<>("Airport Name");
		column_a2.setCellValueFactory(new MapValueFactory<>("airportName"));

		TableColumn<Map, String> column_a3 = new TableColumn<>("Number of Flights");
		column_a3.setCellValueFactory(new MapValueFactory<>("numOfFlights"));
		column_a3.setStyle("-fx-alignment: CENTER;");
		
		tblVwAirportFlights.getColumns().add(column_a1);
		tblVwAirportFlights.getColumns().add(column_a2);
		tblVwAirportFlights.getColumns().add(column_a3);		
		tblVwAirportFlights.setMinWidth(500);


		TableColumn<Map, String> column_b1 = new TableColumn<>("Airport Code");
		column_b1.setCellValueFactory(new MapValueFactory<>("airportCode"));
		column_b1.setStyle("-fx-alignment: CENTER;");
		
		TableColumn<Map, String> column_b2 = new TableColumn<>("Airport Name");
		column_b2.setCellValueFactory(new MapValueFactory<>("airportName"));

		
		tblVwUnusedAirports.getColumns().add(column_b1);
		tblVwUnusedAirports.getColumns().add(column_b2);
		
		GridPane grid = new GridPane();
	    grid.setHgap(30);
	    grid.setVgap(10);
	    grid.setPadding(new Insets(10, 10, 10, 10));
		
	    // Category in column 1, row 1
	    Text category = new Text("Number of flights from each airport:");
	    category.setFont(Font.font("Arial", FontWeight.BOLD, 16));
	    grid.add(category, 0, 0); 

	    // Title in column 2, row 1
	    Text chartTitle = new Text("Unused airports");
	    chartTitle.setFont(Font.font("Arial", FontWeight.BOLD, 16));
	    grid.add(chartTitle, 1, 0);
		
	    // TableView in column 1, row 2
	    grid.add((tblVwAirportFlights), 0, 1);
	    
	    // TableView in column 1, row 2
	    grid.add((tblVwUnusedAirports), 1, 1);
	    
	    final ColumnConstraints col1 = new ColumnConstraints(100, Control.USE_COMPUTED_SIZE, Control.USE_COMPUTED_SIZE);
	    // should grow as much as possible in width for the right table view
	    final ColumnConstraints col2 = new ColumnConstraints(200, Control.USE_COMPUTED_SIZE, Double.MAX_VALUE);
	    col2.setHgrow(Priority.ALWAYS);
	    
	    grid.getColumnConstraints().addAll(col1, col2);
	    
		return grid;
	}
	
	
	public VBox createFlightsAnalysisVBox() {
		
		trTblVwFlightPassengers = new TreeTableView();
		trTblVwFlightPassengers.setColumnResizePolicy((param) -> true );

		// Create tree table view
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn1 = new TreeTableColumn<>("Flight ID");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn2 = new TreeTableColumn<>("Passengers");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn2_1 = new TreeTableColumn<>("Total");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn2_2 = new TreeTableColumn<>("Passenger ID");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn3 = new TreeTableColumn<>("From Airport");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn3_1 = new TreeTableColumn<>("Code");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn3_2 = new TreeTableColumn<>("Name");	
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn4 = new TreeTableColumn<>("To Airport");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn4_1 = new TreeTableColumn<>("Code");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn4_2 = new TreeTableColumn<>("Name");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn5 = new TreeTableColumn<>("Flight\nMileage");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn6 = new TreeTableColumn<>("Departure\nTime");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn7 = new TreeTableColumn<>("Arrival\nTime");
		TreeTableColumn<DisplayFlightPassengerInfo, String> treeTableColumn8 = new TreeTableColumn<>("Flight\nDuration");

		treeTableColumn1.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightID"));
		treeTableColumn2_1.setCellValueFactory(new TreeItemPropertyValueFactory<>("numOfPassengers"));
		treeTableColumn2_1.setStyle("-fx-alignment: CENTER_RIGHT;");
		treeTableColumn2_2.setCellValueFactory(new TreeItemPropertyValueFactory<>("passengerID"));
		treeTableColumn2.getColumns().addAll(treeTableColumn2_1, treeTableColumn2_2);
		treeTableColumn3_1.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromCode"));
		treeTableColumn3_1.setStyle("-fx-alignment: CENTER;");
		treeTableColumn3_2.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromName"));
		treeTableColumn3.getColumns().addAll(treeTableColumn3_1, treeTableColumn3_2);
		treeTableColumn4_1.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToCode"));
		treeTableColumn4_1.setStyle("-fx-alignment: CENTER;");
		treeTableColumn4_2.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToName"));
		treeTableColumn4.getColumns().addAll(treeTableColumn4_1, treeTableColumn4_2);
		treeTableColumn5.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightMileage"));
		treeTableColumn5.setStyle("-fx-alignment: CENTER_RIGHT;");
		treeTableColumn6.setCellValueFactory(new TreeItemPropertyValueFactory<>("depTime"));
		treeTableColumn6.setStyle("-fx-alignment: CENTER_RIGHT;");
		treeTableColumn7.setCellValueFactory(new TreeItemPropertyValueFactory<>("arrTime"));
		treeTableColumn7.setStyle("-fx-alignment: CENTER_RIGHT;");
		treeTableColumn8.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightTime"));
		treeTableColumn8.setStyle("-fx-alignment: CENTER_RIGHT;");
		trTblVwFlightPassengers.getColumns().add(treeTableColumn1);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn2);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn3);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn4);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn5);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn6);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn7);
		trTblVwFlightPassengers.getColumns().add(treeTableColumn8);
		treeTableColumn1.setPrefWidth(110);
		treeTableColumn2_1.setPrefWidth(40);
		treeTableColumn2_2.setPrefWidth(100);
		treeTableColumn3_1.setPrefWidth(50);
		treeTableColumn3_2.setPrefWidth(140);
		treeTableColumn4_1.setPrefWidth(50);
		treeTableColumn4_2.setPrefWidth(140);
		treeTableColumn5.setPrefWidth(70);
		treeTableColumn6.setPrefWidth(70);
		treeTableColumn7.setPrefWidth(70);
		treeTableColumn8.setPrefWidth(70);
		
		return (new VBox(trTblVwFlightPassengers));
	}

	
	public VBox createPassengersAnalysisVBox() {
		
		trTblVwPassengerMileage = new TreeTableView();
		trTblVwPassengerMileage.setColumnResizePolicy((param) -> true );

		// Create tree table view
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn1 = new TreeTableColumn<>("Passenger ID");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn2 = new TreeTableColumn<>("Total Mileage");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn3 = new TreeTableColumn<>("Flight ID");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn4 = new TreeTableColumn<>("From Airport");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn4_1 = new TreeTableColumn<>("Code");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn4_2 = new TreeTableColumn<>("Name");	
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn5 = new TreeTableColumn<>("To Airport");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn5_1 = new TreeTableColumn<>("Code");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn5_2 = new TreeTableColumn<>("Name");
		TreeTableColumn<DisplayPassengerMileageInfo, String> treeTableColumn6 = new TreeTableColumn<>("Flight Mileage");

		treeTableColumn1.setCellValueFactory(new TreeItemPropertyValueFactory<>("passengerID"));
		treeTableColumn2.setCellValueFactory(new TreeItemPropertyValueFactory<>("totMileage"));
		treeTableColumn2.setStyle("-fx-alignment: CENTER_RIGHT;");
		treeTableColumn3.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightID"));
		treeTableColumn4_1.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromCode"));
		treeTableColumn4_1.setStyle("-fx-alignment: CENTER;");
		treeTableColumn4_2.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromName"));
		treeTableColumn4.getColumns().addAll(treeTableColumn4_1, treeTableColumn4_2);
		treeTableColumn5_1.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToCode"));
		treeTableColumn5_1.setStyle("-fx-alignment: CENTER;");
		treeTableColumn5_2.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToName"));
		treeTableColumn5.getColumns().addAll(treeTableColumn5_1, treeTableColumn5_2);
		treeTableColumn6.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightMileage"));
		treeTableColumn6.setStyle("-fx-alignment: CENTER_RIGHT;");
		
		trTblVwPassengerMileage.getColumns().add(treeTableColumn1);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn2);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn3);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn4);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn5);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn6);
		treeTableColumn1.setPrefWidth(120);
		treeTableColumn2.setPrefWidth(90);
		treeTableColumn3.setPrefWidth(90);
		treeTableColumn4_1.setPrefWidth(50);
		treeTableColumn4_2.setPrefWidth(140);
		treeTableColumn5_1.setPrefWidth(50);
		treeTableColumn5_2.setPrefWidth(140);
		treeTableColumn6.setPrefWidth(90);
		
		return (new VBox(trTblVwPassengerMileage));
	}
	
	
	public VBox createLoggingVBox() {

	    TextArea txtAreaLogging = new TextArea();
		txtAreaLogging.setPrefHeight(400);
		txtAreaLogging.setFont(Font.font("Courier New", FontWeight.BOLD, 14));
		
		Logger.getInstance().setTextArea(txtAreaLogging);	
		
		Logger.getInstance().setDebugMode(true);

		return (new VBox(txtAreaLogging));
	}
	
	
	public static void displayAirportsAnalysis(Map mapResult) {
		ObservableList<Map<String, Object>> items = FXCollections.<Map<String, Object>>observableArrayList();

	    Iterator iteratorA1 = mapResult.entrySet().iterator();
	    while(iteratorA1.hasNext()) {
	    	Map.Entry<Object, Integer> entry = (Map.Entry) iteratorA1.next();
	    	//int intFlightCount = entry.getValue().intValue();
			Map<String, Object> item = new HashMap<>();
			item.put("airportCode", entry.getKey());
			item.put("airportName" , getAirportName((String)entry.getKey()));
			item.put("numOfFlights", entry.getValue());
			items.add(item);
	    }
		tblVwAirportFlights.getItems().addAll(items);
		Utilities.autoResizeTableViewColumns(tblVwAirportFlights);
	}
	
	public static void displayUnusedAirportsAnalysis(Map mapResult) {

		ObservableList<Map<String, Object>> items = FXCollections.<Map<String, Object>>observableArrayList();
        Iterator iterator4 = mapResult.entrySet().iterator();
        
        while(iterator4.hasNext()) {
            Map.Entry<Object, Set> entry = (Map.Entry) iterator4.next();
            Iterator airportNames = entry.getValue().iterator();
            
            while(airportNames.hasNext()) {
            	String sAirportCode = (String) airportNames.next();
    			Map<String, Object> item = new HashMap<>();
    			item.put("airportCode", sAirportCode);
    			item.put("airportName", getAirportName(sAirportCode));
    			items.add(item);
            }
    		tblVwUnusedAirports.getItems().addAll(items);
        } 
		Utilities.autoResizeTableViewColumns(tblVwUnusedAirports);

	}
	
	public static void displayFlightsAnalysis(Map mapResult) {
		// Output for the objective (b) & (c)
		ObservableList<Map<String, Object>> items = FXCollections.<Map<String, Object>>observableArrayList();

 		TreeItem trItmTop = new TreeItem();
		Iterator iterator = mapResult.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            FlightPassengerInfo objFP = (FlightPassengerInfo)entry.getValue();
  
         	DisplayFlightPassengerInfo objDisplayFlight = new DisplayFlightPassengerInfo();
         	objDisplayFlight.setFlightID((String)entry.getKey());
         	objDisplayFlight.setNumOfPassengers(Integer.toString(objFP.getNumOfPassengers()));
         	objDisplayFlight.setAirportFromCode(objFP.getAirportFrom());
         	objDisplayFlight.setAirportFromName(getAirportName(objFP.getAirportFrom()));
			objDisplayFlight.setAirportToCode(objFP.getAirportTo());
			objDisplayFlight.setAirportToName(getAirportName(objFP.getAirportTo()));
			objDisplayFlight.setFlightMileage(new DecimalFormat("#,###").format(objFP.getFlightMileage()));
			objDisplayFlight.setDepTime(objFP.getDepTime());
			objDisplayFlight.setArrTime(objFP.getArrTime());
			objDisplayFlight.setFlightTime(objFP.getFlightTime());       
			
        	TreeItem trItmFlight = new TreeItem(objDisplayFlight);
        	
           // List passengers = objFP.getPassengers();
//            for (int i=0;i<passengers.size();i++) {
//            	DisplayFlightPassengerInfo objDisplayPassenger = new DisplayFlightPassengerInfo();          	
//            	objDisplayPassenger.setPassengerID((String)passengers.get(i));
//            	TreeItem trItmPassenger = new TreeItem(objDisplayPassenger);
//            	trItmFlight.getChildren().add(trItmPassenger);
//            }
            
            Iterator passengers = objFP.getPassengers().iterator();
    	    while(passengers.hasNext()) {
	            String passengerID = (String) passengers.next();
	           	DisplayFlightPassengerInfo objDisplayPassenger = new DisplayFlightPassengerInfo();          	
            	objDisplayPassenger.setPassengerID(passengerID);
            	TreeItem trItmPassenger = new TreeItem(objDisplayPassenger);
            	trItmFlight.getChildren().add(trItmPassenger);
    	    } 
    		trItmTop.getChildren().add(trItmFlight);
    		
        } 
        
        trTblVwFlightPassengers.setRoot(trItmTop);
        trTblVwFlightPassengers.setShowRoot(false);
 
	}
	
	public static void displayPassengersAnalysis(Map mapFlightMileage, Map mapPassengerMileage) {
		// Output for the objective (d)
		
		//Sort the passenger records by their total mileage in descending order 
        Set<Entry<String, Double>> set = mapPassengerMileage.entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
 		TreeItem trItmTop = new TreeItem();
        for (Entry<String, Double> entry : list) {
    		DisplayPassengerMileageInfo objPassengerTot = new DisplayPassengerMileageInfo();
    		objPassengerTot.setPassengerID(entry.getKey());
    		objPassengerTot.setTotMileage(new DecimalFormat("#,###").format(entry.getValue()));
       		TreeItem trItmPassengerTot = new TreeItem(objPassengerTot);
       		
            CopyOnWriteArrayList<PassengerTripInfo> lstPTI = (CopyOnWriteArrayList<PassengerTripInfo>) mapFlightMileage.get(entry.getKey());
            for (int i=0; i<lstPTI.size(); i++) {
            	PassengerTripInfo objPTI = lstPTI.get(i);
        		DisplayPassengerMileageInfo objFlightTrip = new DisplayPassengerMileageInfo();
        		objFlightTrip.setFlightID(objPTI.getFlightID());
        		objFlightTrip.setAirportFromCode(objPTI.getAirportFrom());
        		objFlightTrip.setAirportFromName(getAirportName(objPTI.getAirportFrom()));
        		objFlightTrip.setAirportToCode(objPTI.getAirportTo());
        		objFlightTrip.setAirportToName(getAirportName(objPTI.getAirportTo()));
        		objFlightTrip.setFlightMileage(new DecimalFormat("#,###").format(objPTI.getFlightMileage()));
        		
        		TreeItem trItmFlightTrip = new TreeItem(objFlightTrip);
        		trItmPassengerTot.getChildren().add(trItmFlightTrip);
            }
     		trItmTop.getChildren().add(trItmPassengerTot);
        }	
        trTblVwPassengerMileage.setRoot(trItmTop);
        trTblVwPassengerMileage.setShowRoot(false);
        
	}
	
	//-------------------------------------------------------------------
	// Method to kick start all the MapReduce jobs
	//-------------------------------------------------------------------
	public static void startMapReduceJobs(String sAirportDataFile, String sPassengerDataFile) {
		
		// Set mouse pointer to Cursor.WAIT
		Utilities.setCursorWait(stgPrimaryStage.getScene());
		
		// Reset the "Has error" status to false
		Utilities.setErrorStatus(false);
		
		// Load the Airport data from file
		dictAirportInfo = Utilities.loadAirportData(sAirportDataFile);		
		

		//---------------------------------------------------------------------------
		// Run Job 1 for objective (a): No. of flights from each airport
		//---------------------------------------------------------------------------

		// Split the input file into files with smaller partitions with defined size in properties file
		File inputFileJob1 = new File(sPassengerDataFile);
		List<File> splitFilesJob1 = null;
		
		try {	
			splitFilesJob1 = Utilities.splitFile(inputFileJob1, Integer.parseInt(configProps.getProperty("partition.size")));
		} catch (Exception e) {
			Logger.getInstance().logError("Error in splitting the input file: " + e);		
			return;
		}
		
		// Assign each split file to a mapper instance to run concurrently
		ArrayList<String> inFilesJob1 = new ArrayList<String>();
		for (File sf: splitFilesJob1) {
			System.out.println("Filename:" + sf.getName());
			inFilesJob1.add(sf.getName());
		}
		
		runJob1(inFilesJob1);

		
		//---------------------------------------------------------------------------
		// Run Job 2 for objective (a): Unused airports
		//---------------------------------------------------------------------------
		
		// Split the input file into files with smaller partitions with defined size in properties file
		File inputFileJob2 = new File(configProps.getProperty("job1.outputfile.2"));
		List<File> splitFilesJob2 = null;
		
		try {	
			splitFilesJob2 = Utilities.splitFile(inputFileJob2, Integer.parseInt(configProps.getProperty("partition.size")));
		} catch (Exception e) {
			Logger.getInstance().logError("Error in splitting the input file: " + e);		
			return;
		}
		
		// Assign each split file to a mapper instance to run concurrently
		ArrayList<String> inFilesJob2 = new ArrayList<String>();

		for (File sf: splitFilesJob2) {
			System.out.println("Filename:" + sf.getName());
			inFilesJob2.add(sf.getName());
		}
		
		runJob2(inFilesJob2);

		//---------------------------------------------------------------------------
		// Run Job 3 for objective (b): List of flights with passenger list, 
		// (c): No. of passengers on each flight, and (d): Line-of-sight miles for each flight
		//---------------------------------------------------------------------------
		runJob3(inFilesJob1);
		
		//---------------------------------------------------------------------------
		// Run Job 4 for objective (d) Total mileage traveled by each passenger, and passenger having highest mileage
		//---------------------------------------------------------------------------
		runJob4(inFilesJob1);
		
		// Set mouse pointer back to Cursor.DEFAULT
		Utilities.setCursorDefault(stgPrimaryStage.getScene());
		
		// If error/warning occurred during running the jobs, alert user to check the log messages
		if (Utilities.getErrorStatus()) {
			Alert a = new Alert(AlertType.WARNING);
			a.setContentText("Error(s) or warning(s) raised when running the jobs, please check the Log Messages.");
            a.show();
		}
	}
	
	//-------------------------------------------------------------------	
	// Job 1: Flights from each airport (Objective a)
	//-------------------------------------------------------------------
	public static void runJob1(ArrayList<String> inFiles) {
		
		Config config = new Config(inFiles, AirportFlightMapper.class, AirportFlightReducer.class, false);
		// add the airport data into the config

		Job jobAirportFlights = new Job(config);
		jobAirportFlights.addRefData("AirportInfo", ((HashMap)dictAirportInfo).clone());
		
		setInvalidAirportCode.clear();
		
		try {
			jobAirportFlights.addJobResultBucket("UsedAirports");
			jobAirportFlights.addJobResultBucket("FlightCount");	
			jobAirportFlights.run();
		} catch (Exception e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		// Export the job results to files
		exportJob1Results(jobAirportFlights);
		
		// Display the job result in the GUI for the objective (a)
		displayAirportsAnalysis(jobAirportFlights.getJobResult("FlightCount"));		
		
	}
	
	//-------------------------------------------------------------------
	// Job 2: List of unused airport (Objective a)
	//-------------------------------------------------------------------
	public static void runJob2(ArrayList<String> inFiles) {	
		
		Config config = new Config(inFiles, UnusedAirportMapper.class, UnusedAirportReducer.class, true);
		Job jobUnusedAirports = new Job(config);
		jobUnusedAirports.addRefData("AirportInfo", ((HashMap)dictAirportInfo).clone());
		try {
			jobUnusedAirports.run();
		} catch (Exception e) {
			Logger.getInstance().logError(e.getMessage());
		}
	
		exportJob2Results(jobUnusedAirports);
	        
		displayUnusedAirportsAnalysis(jobUnusedAirports.getJobResult());  
	}

	//-------------------------------------------------------------------
	// Job 3: List of flights and their passengers (Objective b)
	//-------------------------------------------------------------------
	public static void runJob3(ArrayList<String> inFiles) {

		Config config = new Config(inFiles, FlightPassengerMapper.class, FlightPassengerReducer.class, false);
		Job jobFlightPassengers = new Job(config);
		jobFlightPassengers.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		
		try {
			jobFlightPassengers.run();
		} catch (Exception e) {
			Logger.getInstance().logError(e.getMessage());
		}
	
		exportJob3Results(jobFlightPassengers);
	    
		displayFlightsAnalysis(jobFlightPassengers.getJobResult());
	
	}

	//-------------------------------------------------------------------
	// Job 4: List of passengers with their total flight mileages
	//-------------------------------------------------------------------
	public static void runJob4(ArrayList<String> inFiles) {

		Config config = new Config(inFiles, PassengerMileageMapper.class, PassengerMileageReducer.class, false);
		Job jobPassengerMileage = new Job(config);
		jobPassengerMileage.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		try {
			jobPassengerMileage.addJobResultBucket("FlightMileage");
			jobPassengerMileage.addJobResultBucket("PassengerMileage");	
			jobPassengerMileage.run();
		} catch (Exception e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		exportJob4Results(jobPassengerMileage);
	       
		displayPassengersAnalysis(jobPassengerMileage.getJobResult("FlightMileage"), jobPassengerMileage.getJobResult("PassengerMileage"));
	}

	public static void exportJob1Results(Job jobAirportFlights) {
		// Write output file #1.1
		// Write the list of used airports to output file for job chaining
        Iterator iterator1 = jobAirportFlights.getJobResult("FlightCount").entrySet().iterator();
        
        PrintWriter writer1 = null;
		try {
			writer1 = new PrintWriter(new File(configProps.getProperty("job1.outputfile.1")));
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		// Write column headers into the output file
		writer1.println("AirportCode,AirportName,NumOfFlights");
		
	    while(iterator1.hasNext()) {
	            Map.Entry<Object, Integer> entry = (Map.Entry) iterator1.next();
	            writer1.println(entry.getKey() + "," + getAirportName((String)entry.getKey()) + "," + entry.getValue());
	        } 
        writer1.close();
        
        
		// Write output file #1.2
		// Write the list of used airports to output file for job chaining
        Iterator iterator2 = jobAirportFlights.getJobResult("UsedAirports").entrySet().iterator();
        
        PrintWriter writer2 = null;
		try {
			writer2 = new PrintWriter(new File(configProps.getProperty("job1.outputfile.2")));
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		// Write column headers into the output file
		writer2.println("AirportFromCode,AirportToCode");
		
	    while(iterator2.hasNext()) {
	            Map.Entry<Object, FlightTripInfo> entry = (Map.Entry) iterator2.next();
	            FlightTripInfo objFTI = entry.getValue();
	            // Write entry to the output file
	            writer2.println(objFTI.getAirportFrom() + "," + objFTI.getAirportTo());
	        } 
        writer2.close();

        
		// Perform error correction suggestion on invalid airport codes
        String sOutFile = configProps.getProperty("job1.datacorrection.suggestion.file", "5_airportcode_correction_suggestion.txt");
        PrintWriter writer3 = null;
		try {
			writer3 = new PrintWriter(new File(sOutFile));
	        Iterator iter = setInvalidAirportCode.iterator();
	        while (iter.hasNext()) {
	        	String sInvalidCode = (String)iter.next();
	            List lstSuggestions = Utilities.getSuggestedAirportCodes(sInvalidCode);
	        	writer3.print("Invalid Airport Code: '" + sInvalidCode + 
	        			"', Suggested Code(s): ");
	        	if(lstSuggestions.size()==0)
	        		writer3.println("N/A");
	        	else {
	        		for (int i=0; i<lstSuggestions.size(); i++) {
	        			writer3.print(lstSuggestions.get(i));
	        			if(i<lstSuggestions.size()-1)
	        				writer3.print(", ");
	        			else
	        				writer3.println("");
	        		}
	        	}
	        }
	        writer3.close();
		} catch (FileNotFoundException e) {
			System.err.println(e);
		}
        		
	}
	
	
	public static void exportJob2Results(Job jobUnusedAirports) {
		
		// Write output file #2
        Iterator iterator1 = jobUnusedAirports.getJobResult().entrySet().iterator();
        
        PrintWriter writer1 = null;
		try {
			writer1 = new PrintWriter(new File(configProps.getProperty("job2.outputfile.1")));
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		// Print column headers into the output file
		writer1.println("AirportCode,AirportName");
		
	    while(iterator1.hasNext()) {
	    	Map.Entry<Object, Set> entry = (Map.Entry) iterator1.next();
	    	Iterator airportNames = entry.getValue().iterator();

	    	while(airportNames.hasNext()) {
	    		String sAirportCode = (String) airportNames.next();
	    		writer1.println(sAirportCode + "," + getAirportName(sAirportCode));
	    	}
	    } 
        writer1.close();		
	}
	
	public static void exportJob3Results(Job jobFlightPassengers) {
		
		// Write output file #3
        Iterator iterator1 = jobFlightPassengers.getJobResult().entrySet().iterator();
        
        PrintWriter writer1 = null;
		try {
			writer1 = new PrintWriter(new File(configProps.getProperty("job3.outputfile.1")));
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		// Print column headers into the output file
		writer1.println("FlightID,TotPassenger,PassengerID,AirportFromCode,AirportFromName,AirportToCode,AirportToName,FlightMileage,DepTime,ArrTime,FlightDuration");

		Iterator iterator = jobFlightPassengers.getJobResult().entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            FlightPassengerInfo objFP = (FlightPassengerInfo)entry.getValue();
  
            writer1.print((String)entry.getKey()+",");
            writer1.print(Integer.toString(objFP.getNumOfPassengers())+",,");
            writer1.print(objFP.getAirportFrom()+",");
            writer1.print(getAirportName(objFP.getAirportFrom())+",");
            writer1.print(objFP.getAirportTo()+",");
            writer1.print(getAirportName(objFP.getAirportTo())+",");
            writer1.print(objFP.getFlightMileage()+",");
            writer1.print(objFP.getDepTime()+",");
            writer1.print(objFP.getArrTime()+",");
            writer1.println(objFP.getFlightTime());       
			     	
//            List passengers = objFP.getPassengers();
//            for (int i=0;i<passengers.size();i++) {          	
//            	writer1.println(",,"+passengers.get(i));
//            }  
            
            Iterator passengers = objFP.getPassengers().iterator();
    	    while(passengers.hasNext()) {
    	    	String passengerID = (String) passengers.next();
              	writer1.println(",,"+passengerID);
    	    }
        } 
        writer1.close();
	}
	
	public static void exportJob4Results(Job jobPassengerMileage) {

		// Write output file #4        
        PrintWriter writer1 = null;
		try {
			writer1 = new PrintWriter(new File(configProps.getProperty("job4.outputfile.1")));
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError(e.getMessage());
		}
		
		//Sort the passenger records by their total mileage in descending order 
        Set<Entry<String, Double>> set = jobPassengerMileage.getJobResult("PassengerMileage").entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        writer1.println("PassengerID,TotMileage,FlightID,AirportFromCode,AirportFromName,AirportToCode,AirportToName,FlightMileage");
        for (Entry<String, Double> entry : list) {
        	writer1.println(entry.getKey()+","+entry.getValue());
   
        	CopyOnWriteArrayList<PassengerTripInfo> lstPTI = (CopyOnWriteArrayList<PassengerTripInfo>) jobPassengerMileage.getJobResult("FlightMileage").get(entry.getKey());
            for (int i=0; i<lstPTI.size(); i++) {
            	PassengerTripInfo objPTI = lstPTI.get(i);
        		writer1.print(",,"+objPTI.getFlightID()+",");
        		writer1.print(objPTI.getAirportFrom()+",");
        		writer1.print(getAirportName(objPTI.getAirportFrom())+",");
        		writer1.print(objPTI.getAirportTo()+",");
        		writer1.print(getAirportName(objPTI.getAirportTo())+",");
        		writer1.println(objPTI.getFlightMileage());
            }
        }	
		writer1.close();
	}
	
	
	public static String getAirportName(String sCode) {
		if (dictAirportInfo!=null) {
			AirportInfo objAI = (AirportInfo)dictAirportInfo.get(sCode);
			if (objAI==null)
				return "";
			else
				return objAI.getName();
		} else
			return "";
	}

}
