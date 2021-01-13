/**
 * 
 */
package uk.ac.reading.csmcc16;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javafx.application.Application;
import javafx.application.ConditionalFeature;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.scene.Group;
import javafx.scene.PerspectiveCamera;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.Control;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TabPane.TabClosingPolicy;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TreeItemPropertyValueFactory;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Background;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Box;
import javafx.scene.shape.CullFace;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.scene.transform.Rotate;

import uk.ac.reading.csmcc16.mapReduce.*;
import uk.ac.reading.csmcc16.mapReduce.core.*;

/**
 * CSMCC16 Coursework
 * Description: Develop a Software Prototype of a MapReduce-like system
 * @author jacklau
 *
 */
public class FlightsFlowAnalyser extends Application {

	static Properties configProps;
	static String propFileName = "csmcc16.properties";
	static Map<String, Object> dictAirportInfo = new HashMap<String, Object>();
	
	// JavaFX objects
	TextField txtFldAirportDataFile;
	TextField txtFldPassengerDataFile;
//	TableView tblVwAirportsAnalysis;
	static TableView tblVwAirportFlights;// = new TableView();
	static TableView tblVwUnusedAirports;// = new TableView();
	static TableView tblVwFlightPassengers;
	static TreeTableView trTblVwFlightPassengers;
	static TreeTableView trTblVwPassengerMileage;
	TabPane tbPnMain;
	Stage stgPrimaryStage;
	
	@Override
	public void start(Stage primaryStage) {
		try {						
			stgPrimaryStage = primaryStage;

			// Create a tab control and all 4 tabs to it
			tbPnMain = new TabPane();
			tbPnMain.setTabClosingPolicy(TabClosingPolicy.UNAVAILABLE);
			tbPnMain.getTabs().add(new Tab("Start", createStartVBox()));
			tbPnMain.getTabs().add(new Tab("Airports Analysis", createAirportsAnalysisGridPane()));
			tbPnMain.getTabs().add(new Tab("Flights Analysis", createFlightsAnalysisVBox()));
			tbPnMain.getTabs().add(new Tab("Passenger Analysis", createPassengersAnalysisVBox()));
		
			primaryStage.setScene(new Scene(new VBox(tbPnMain), 800, 600));
			primaryStage.setTitle("Passenger Data Analysis (MapReduce Prototype)");
			primaryStage.show();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public VBox createStartVBox() {
		
	    Image imgChecked = new Image(getClass().getClassLoader().getResourceAsStream("green_check.png"));
	    ImageView ivGreenCheck1 = new ImageView(imgChecked);
	    ivGreenCheck1.setFitHeight(30);
	    ivGreenCheck1.setFitWidth(30);
	    ivGreenCheck1.setVisible(false);
	    
	    ImageView ivGreenCheck2 = new ImageView(imgChecked);
	    ivGreenCheck2.setFitHeight(30);
	    ivGreenCheck2.setFitWidth(30);
	    ivGreenCheck2.setVisible(false);
	    
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
//				Alert a = new Alert(AlertType.INFORMATION);
//				a.setContentText(txtFldAirportDataFile.getText());
//				a.show();
			    ivGreenCheck1.setVisible(true);
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
//				Alert a = new Alert(AlertType.INFORMATION);
//				a.setContentText(txtFldPassengerDataFile.getText());
//				a.show();
			    ivGreenCheck2.setVisible(true);
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
//				Alert a = new Alert(AlertType.INFORMATION);
//				a.setContentText("Ready to go!");
//				a.show();
//				
				// Clear the result if any
				tblVwAirportFlights.getItems().clear();
				tblVwUnusedAirports.getItems().clear();
				trTblVwPassengerMileage.setRoot(null);
				//tableView.getItems().clear();
				//treeTableView.setRoot(null);
				
				// Perform MapReduce stuff
				buttonStartClicked(txtFldAirportDataFile.getText(), txtFldPassengerDataFile.getText());
				// Display the first result tab
				tbPnMain.getSelectionModel().select(1);
			}
		});

		HBox hbox1 = new HBox(10, textField1, ivGreenCheck1);
		hbox1.setAlignment(Pos.CENTER);
		HBox hbox2 = new HBox(10, textField2, ivGreenCheck2);
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

		TableColumn<Map, String> column_a2 = new TableColumn<>("Airport Name");
		column_a2.setCellValueFactory(new MapValueFactory<>("airportName"));

		TableColumn<Map, String> column_a3 = new TableColumn<>("Number of Flights");
		column_a3.setCellValueFactory(new MapValueFactory<>("numOfFlights"));
		
		tblVwAirportFlights.getColumns().add(column_a1);
		tblVwAirportFlights.getColumns().add(column_a2);
		tblVwAirportFlights.getColumns().add(column_a3);
		
		//tblVwAirportFlights.setPlaceholder(new Label("No rows to display"));
		tblVwAirportFlights.setMinWidth(400);


		TableColumn<Map, String> column_b1 = new TableColumn<>("Airport Code");
		column_b1.setCellValueFactory(new MapValueFactory<>("airportCode"));

		TableColumn<Map, String> column_b2 = new TableColumn<>("Airport Name");
		column_b2.setCellValueFactory(new MapValueFactory<>("airportName"));

		
		tblVwUnusedAirports.getColumns().add(column_b1);
		tblVwUnusedAirports.getColumns().add(column_b2);
		
	
		ObservableList<Map<String, Object>> items_b =
			    FXCollections.<Map<String, Object>>observableArrayList();
		
		Map<String, Object> itemb1 = new HashMap<>();
		itemb1.put("airportCode", "HKG");
		itemb1.put("airportName" , "Hong Kong International Airport");
		
		for (int i=0; i<100;i++)
			items_b.add(itemb1);

		tblVwUnusedAirports.getItems().addAll(items_b);
		
		GridPane grid = new GridPane();
	    grid.setHgap(30);
	    grid.setVgap(10);
	    grid.setPadding(new Insets(10, 10, 10, 10));
//		grid.setFillWidth(tblVwUnusedAirports, true);
		
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
	    // should grow as much as possible in width
	    final ColumnConstraints col2 = new ColumnConstraints(200, Control.USE_COMPUTED_SIZE, Double.MAX_VALUE);
	    col2.setHgrow(Priority.ALWAYS);
	    
//	    gridPane.getRowConstraints().add(row);
	    grid.getColumnConstraints().addAll(col1, col2);
	    
		return grid;
	}
	
	
	public VBox createFlightsAnalysisVBox() {
			// Create tree table view
		
		tblVwFlightPassengers = new TableView();
	
		// Set the table view to resize columns automatically
		tblVwFlightPassengers.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
		
		// Use Map to add data
		
		TableColumn<Map, String> column_a1 = new TableColumn<>("Flight ID");
		column_a1.setCellValueFactory(new MapValueFactory<>("flightID"));

		TableColumn<Map, String> column_a2 = new TableColumn<>("Number of Passengers");
		column_a2.setCellValueFactory(new MapValueFactory<>("numOfPassengers"));

		TableColumn<Map, String> column_a3 = new TableColumn<>("Airport From");
		column_a3.setCellValueFactory(new MapValueFactory<>("airportFrom"));

		TableColumn<Map, String> column_a4 = new TableColumn<>("Airport To");
		column_a4.setCellValueFactory(new MapValueFactory<>("airportTo"));
		
		TableColumn<Map, String> column_a5 = new TableColumn<>("Flight Mileage");
		column_a5.setCellValueFactory(new MapValueFactory<>("flightMileage"));		
		
		tblVwFlightPassengers.getColumns().add(column_a1);
		tblVwFlightPassengers.getColumns().add(column_a2);
		tblVwFlightPassengers.getColumns().add(column_a3);
		tblVwFlightPassengers.getColumns().add(column_a4);
		tblVwFlightPassengers.getColumns().add(column_a5);

		//			TreeTableView<FlightPassengerInfo> treeTableView = new TreeTableView<FlightPassengerInfo>();
//			TreeTableColumn<FlightPassengerInfo, String> treeTableColumn1 = new TreeTableColumn<>("Flight ID");
//			TreeTableColumn<FlightPassengerInfo, String> treeTableColumn2 = new TreeTableColumn<>("Airport From");
//	
//			treeTableColumn1.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightID"));
//			treeTableColumn2.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFrom"));
//	
//			treeTableView.getColumns().add(treeTableColumn1);
//			treeTableView.getColumns().add(treeTableColumn2);
//	
//			String passengerID = "PUD8209OG3";
//			String flightID = "PME8178S";
//			String airportFrom = "DEN";
//			String airportTo = "PEK";
//			String depTime = "13:20";
//			String arrTime = "22:00";
//			String flightTime = "7:40";
//			
//			FlightPassengerInfo fpInfo = new FlightPassengerInfo(flightID, airportFrom, airportTo, depTime, arrTime, flightTime);
//			FlightPassengerInfo fpInfo2 = new FlightPassengerInfo("XXXX", airportFrom, airportTo, depTime, arrTime, flightTime);
//			TreeItem mercedes1 = new TreeItem(fpInfo);
//			TreeItem mercedes2 = new TreeItem(fpInfo2);
//			//TreeItem mercedes3 = new TreeItem(new Car("Mercedes", "CLA 200"));
//	
//			TreeItem mercedes = new TreeItem(fpInfo);
//			mercedes.getChildren().add(mercedes1);
//			mercedes.getChildren().add(mercedes2);
//	
//	//		TreeItem audi1 = new TreeItem(new Car("Audi", "A1"));
//	//		TreeItem audi2 = new TreeItem(new Car("Audi", "A5"));
//	//		TreeItem audi3 = new TreeItem(new Car("Audi", "A7"));
//	//
//	//		TreeItem audi = new TreeItem(new Car("Audi", "..."));
//	//		audi.getChildren().add(audi1);
//	//		audi.getChildren().add(audi2);
//	//		audi.getChildren().add(audi3);
//	
//			TreeItem cars = new TreeItem(fpInfo);
//	//		cars.getChildren().add(audi);
//			cars.getChildren().add(mercedes);
//	
//			treeTableView.setRoot(cars);
			
			return (new VBox(tblVwFlightPassengers));
		}

	public VBox createPassengersAnalysisVBox() {
		
		trTblVwPassengerMileage = new TreeTableView();
		
		// Create tree table view
//		TreeTableView<FlightPassengerInfo> treeTableView = new TreeTableView<FlightPassengerInfo>();
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn1 = new TreeTableColumn<>("Passenger ID");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn2 = new TreeTableColumn<>("Total Mileage");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn3 = new TreeTableColumn<>("Flight ID");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn4 = new TreeTableColumn<>("Airport From Code");		
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn5 = new TreeTableColumn<>("Airport From Name");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn6 = new TreeTableColumn<>("Airport To Code");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn7 = new TreeTableColumn<>("Airport To Name");
		TreeTableColumn<DisplayPassengerMileage, String> treeTableColumn8 = new TreeTableColumn<>("Flight Mileage");

		treeTableColumn1.setCellValueFactory(new TreeItemPropertyValueFactory<>("passengerID"));
		treeTableColumn2.setCellValueFactory(new TreeItemPropertyValueFactory<>("totMileage"));
		treeTableColumn3.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightID"));
		treeTableColumn4.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromCode"));
		treeTableColumn5.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportFromName"));
		treeTableColumn6.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToCode"));
		treeTableColumn7.setCellValueFactory(new TreeItemPropertyValueFactory<>("airportToName"));
		treeTableColumn8.setCellValueFactory(new TreeItemPropertyValueFactory<>("flightMileage"));

		trTblVwPassengerMileage.getColumns().add(treeTableColumn1);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn2);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn3);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn4);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn5);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn6);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn7);
		trTblVwPassengerMileage.getColumns().add(treeTableColumn8);
		

		
		return (new VBox(trTblVwPassengerMileage));
		
//		TableView tableView = new TableView();

//		TableColumn<FlightPassengerInfo, String> column1 = new TableColumn<>("Passenger ID");
//		column1.setCellValueFactory(new PropertyValueFactory<>("flightID"));
//
//
//		TableColumn<FlightPassengerInfo, String> column2 = new TableColumn<>("Flight ID");
//		column2.setCellValueFactory(new PropertyValueFactory<>("flightID"));
//
//		TableColumn<FlightPassengerInfo, String> column3 = new TableColumn<>("Airport From");
//		column3.setCellValueFactory(new PropertyValueFactory<>("airportFrom"));
//		
//		TableColumn<FlightPassengerInfo, String> column4 = new TableColumn<>("Airport To");
//		column4.setCellValueFactory(new PropertyValueFactory<>("airportTo"));
//		TableColumn<FlightPassengerInfo, String> column5 = new TableColumn<>("Departure Time");
//		column5.setCellValueFactory(new PropertyValueFactory<>("depTime"));
//		TableColumn<FlightPassengerInfo, String> column6 = new TableColumn<>("Flight Duration");
//		column6.setCellValueFactory(new PropertyValueFactory<>("flightTime"));
		
//		String passengerID = "PUD8209OG3";
//		String flightID = "PME8178S";
//		String airportFrom = "DEN";
//		String airportTo = "PEK";
//		String depTime = "13:20";
//		String arrTime = "22:00";
//		String flightTime = "7:40";
//		
//		FlightPassengerInfo fpInfo = new FlightPassengerInfo(flightID, airportFrom, airportTo, depTime, arrTime, flightTime);
//		FlightPassengerInfo fpInfo2 = new FlightPassengerInfo("XXXX", airportFrom, airportTo, depTime, arrTime, flightTime);
//
//		// Use Map to add data
//		
//		TableColumn<Map, String> column1 = new TableColumn<>("Passenger ID");
//		column1.setCellValueFactory(new MapValueFactory<>("passengerID"));
//
//		TableColumn<Map, String> column2 = new TableColumn<>("Flight ID");
//		column2.setCellValueFactory(new MapValueFactory<>("flightID"));
//
//		TableColumn<Map, String> column3 = new TableColumn<>("Airport From");
//		column3.setCellValueFactory(new MapValueFactory<>("airportFrom"));
//		
//		TableColumn<Map, String> column4 = new TableColumn<>("Airport To");
//		column4.setCellValueFactory(new MapValueFactory<>("airportTo"));
//		
//		TableColumn<Map, String> column5 = new TableColumn<>("Departure Time");
//		column5.setCellValueFactory(new MapValueFactory<>("depTime"));
//		
//		TableColumn<Map, String> column6 = new TableColumn<>("Flight Duration");
//		column6.setCellValueFactory(new MapValueFactory<>("flightTime"));
//		
//		tableView.getColumns().add(column1);
//		tableView.getColumns().add(column2);
//		tableView.getColumns().add(column3);
//		tableView.getColumns().add(column4);
//		tableView.getColumns().add(column5);
//		tableView.getColumns().add(column6);
//		
//		ObservableList<Map<String, Object>> items =
//			    FXCollections.<Map<String, Object>>observableArrayList();
//		
//		Map<String, Object> item1 = new HashMap<>();
//		item1.put("passengerID", passengerID);
//		item1.put("flightID" , flightID);
//		item1.put("airportFrom", airportFrom);
//		item1.put("airportTo" , airportTo);
//		item1.put("depTime", depTime);
//		item1.put("arrTime" , arrTime);
//		item1.put("flightTime" , flightTime);
//		
//		for (int i=0; i<100;i++)
//			items.add(item1);
//
//		tableView.getItems().addAll(items);
//		tableView.setPlaceholder(new Label("No rows to display"));
//		
//		return(new VBox(tableView));
	}
	
	public static void displayAirportFlightsAnalysis(Map mapResult) {
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
	}
	
	public static void displayUnusedAirportsAnalysis(Map mapResult) {
		// Output for the objective (a) Unused airports
//		System.out.println("=================================================================");
//		System.out.println("Objective (a): Unused Airports");
//		System.out.println("=================================================================");
		
		ObservableList<Map<String, Object>> items = FXCollections.<Map<String, Object>>observableArrayList();
        Iterator iterator4 = mapResult.entrySet().iterator();
        while(iterator4.hasNext()) {
            Map.Entry<Object, Set> entry = (Map.Entry) iterator4.next();
            Iterator airportNames = entry.getValue().iterator();
 //           System.out.println("Unused airports:");
            while(airportNames.hasNext()) {
 //           	System.out.println(airportNames.next());
            	String sAirportCode = (String) airportNames.next();
    			Map<String, Object> item = new HashMap<>();
    			item.put("airportCode", sAirportCode);
    			item.put("airportName", getAirportName(sAirportCode));
    			items.add(item);
            }
    		tblVwUnusedAirports.getItems().addAll(items);
        } 
		
		
//        Iterator iteratorA2 = mapResult.entrySet().iterator();
//        
//        PrintWriter writer = null;
//		try {
//			writer = new PrintWriter(new File("airports.csv"));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	    while(iteratorA2.hasNext()) {
//	            Map.Entry<Object, FlightTripInfo> entry = (Map.Entry) iteratorA2.next();
//	            FlightTripInfo objFTI = entry.getValue();
//	            System.out.println("AirportFrom=" + objFTI.getAirportFrom() + ", AirportTo=" + objFTI.getAirportTo());
//	            // Write to the output file
//	            writer.println(objFTI.getAirportFrom() + "," + objFTI.getAirportTo());
//	        } 
//        writer.close();	
	}
	
	public static void displayFlightPassengersAnalysis(Map mapResult) {
		// Output for the objective (b) & (c)
		ObservableList<Map<String, Object>> items = FXCollections.<Map<String, Object>>observableArrayList();
		
//		System.out.println("=================================================================");
//		System.out.println("Objective (b) & (c) : Flight, Passengers");
//		System.out.println("=================================================================");
        Iterator iterator = mapResult.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            FlightPassengerInfo objFP = (FlightPassengerInfo)entry.getValue();
//            System.out.println("Flight ID: " + entry.getKey() + " (" + objFP.getNumOfPassengers() + " passengers)");
//            System.out.println("From airport: " + objFP.getAirportFrom());
//            System.out.println("To airport: " + objFP.getAirportTo());
//            System.out.println("Departure time: " + objFP.getDepTime());
//            System.out.println("Arrival time: " + objFP.getArrTime());
//            System.out.println("Flight time: " + objFP.getFlightTime());
            List passengers = objFP.getPassengers();
//           for (int i=0; i<passengers.size(); i++) {
//            	System.out.println(passengers.get(i));
//            }
//            System.out.println("-------------------------------------------------------");
			Map<String, Object> item = new HashMap<>();
			item.put("flightID", entry.getKey());
			item.put("numOfPassengers" , objFP.getNumOfPassengers());
			item.put("airportFrom", objFP.getAirportFrom());
			item.put("airportFromName", getAirportName(objFP.getAirportFrom()));
			item.put("airportTo", objFP.getAirportTo());
			item.put("airportToName", getAirportName(objFP.getAirportTo()));
			item.put("flightMileage", 10000.0);
			items.add(item);
        } 
		tblVwFlightPassengers.getItems().addAll(items);
	}
	
	public static void displayPassengerMileageAnalysis(Map mapFlightMileage, Map mapPassengerMileage) {
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
//		System.out.println("=================================================================");
//		System.out.println("Objective (d): Passenger Mileage");
//		System.out.println("=================================================================");
		
 		TreeItem trItmTop = new TreeItem();
        for (Entry<String, Double> entry : list) {
 //           System.out.println(entry.getKey() + ": " + entry.getValue());
    		DisplayPassengerMileage objPassengerTot = new DisplayPassengerMileage();
    		objPassengerTot.setPassengerID(entry.getKey());
    		objPassengerTot.setTotMileage(entry.getValue());
       		TreeItem trItmPassengerTot = new TreeItem(objPassengerTot);
       		
            CopyOnWriteArrayList<PassengerTripInfo> lstPTI = (CopyOnWriteArrayList<PassengerTripInfo>) mapFlightMileage.get(entry.getKey());
            for (int i=0; i<lstPTI.size(); i++) {
            	PassengerTripInfo objPTI = lstPTI.get(i);
  //          	System.out.println("\t"+objPTI.getFlightID()+": "+objPTI.getFlightMileage());
        		
        		DisplayPassengerMileage objFlightTrip = new DisplayPassengerMileage();
        		objFlightTrip.setFlightID(objPTI.getFlightID());
        		objFlightTrip.setAirportFromCode(objPTI.getAirportFrom());
        		objFlightTrip.setAirportFromName(getAirportName(objPTI.getAirportFrom()));
        		objFlightTrip.setAirportToCode(objPTI.getAirportTo());
        		objFlightTrip.setAirportToName(getAirportName(objPTI.getAirportTo()));
        		objFlightTrip.setFlightMileage(objPTI.getFlightMileage());
        		
        		TreeItem trItmFlightTrip = new TreeItem(objFlightTrip);
        		trItmPassengerTot.getChildren().add(trItmFlightTrip);

//        		TreeItem cars = new TreeItem(fpInfo);
//        		cars.getChildren().add(audi);
 //       		cars.getChildren().add(mercedes);

            }
     		trItmTop.getChildren().add(trItmPassengerTot);
        }	
        trTblVwPassengerMileage.setRoot(trItmTop);
        trTblVwPassengerMileage.setShowRoot(false);
	}
	
	public static void main(String[] args) {

		System.out.println("FlightsFlowAnalyser");
		System.out.println("java.runtime.version: " + System.getProperty("java.runtime.version", "(undefined)"));
		System.out.println("javafx.version: " + System.getProperty("javafx.version", "(undefined)"));
		
		// Load the system properties from file
		try {
			configProps = Utilities.loadProperties(propFileName);
		} catch (Exception e) {
			System.err.println("Exception in loading properties file: " + e);			
			return;
		}
		
		// JavaFX will then call start(Stage) in this Class		
		launch(args);

	}
	
	public static void buttonStartClicked(String sAirportDataFile, String sPassengerDataFile) {
		
		// Load the Airport data from file
		dictAirportInfo = Utilities.loadAirportData(sAirportDataFile);		
		
		// Split the input file into files with smaller partitions with defined size in properties file
		File inputFile = new File(sPassengerDataFile);
		
		List<File> splitFiles = null;
		
		try {	
			splitFiles = Utilities.splitFile(inputFile, Integer.parseInt(configProps.getProperty("partition.size")));
		} catch (Exception e) {
			System.err.println("Error in splitting the input file: " + e);		
			return;
		}
		
		// Assign each split file to a mapper instance to run concurrently
		ArrayList<String> inFiles = new ArrayList<String>();
		for (File sf: splitFiles) {
			System.out.println("Filename:" + sf.getName());
			inFiles.add(sf.getName());
		}
		
		// Run Job 1 for objective (a)
		runJob1(inFiles);
		
		ArrayList<String> inFilesJob2 = new ArrayList<String>();
		inFilesJob2.add("airports.csv");
		runJob2(inFilesJob2);
		
		runJob3(inFiles);
		
		runJob4(inFiles);
		
	}
	
	// Job 1: Flights from each airport
	public static void runJob1(ArrayList<String> inFiles) {
		//-------------------------------------------------------------------
		// Create a job for objective (a)
		//-------------------------------------------------------------------
		Config config2 = new Config(inFiles, AirportFlightMapper.class, AirportFlightReducer.class);
		// add the airport data into the config

		Job jobAirportFlights = new Job(config2);
		try {
			jobAirportFlights.addJobResultBucket("UsedAirports");
			jobAirportFlights.addJobResultBucket("FlightCount");			
			jobAirportFlights.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Output for the objective (a)
		displayAirportFlightsAnalysis(jobAirportFlights.getJobResult("FlightCount"));
		
//        Iterator iteratorA1 = jobAirportFlights.getJobResult("FlightCount").entrySet().iterator();
//        
//		System.out.println("=================================================================");
//		System.out.println("Objective (a): Airport, Flight Count");
//		System.out.println("=================================================================");
//        while(iteratorA1.hasNext()) {
//            Map.Entry<Object, Integer> entry = (Map.Entry) iteratorA1.next();
//            int intFlightCount = entry.getValue().intValue();
//            System.out.println("Airport: " + entry.getKey() + " (" + intFlightCount + " flights)");
//        } 

		//TODO: relocate this part of code
		// Write to output file for job chaining
        Iterator iteratorA2 = jobAirportFlights.getJobResult("UsedAirports").entrySet().iterator();
        
        PrintWriter writer = null;
		try {
			writer = new PrintWriter(new File("airports.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    while(iteratorA2.hasNext()) {
	            Map.Entry<Object, FlightTripInfo> entry = (Map.Entry) iteratorA2.next();
	            FlightTripInfo objFTI = entry.getValue();
//	            System.out.println("AirportFrom=" + objFTI.getAirportFrom() + ", AirportTo=" + objFTI.getAirportTo());
	            // Write to the output file
	            writer.println(objFTI.getAirportFrom() + "," + objFTI.getAirportTo());
	        } 
        writer.close();
        
	}
	
	// Job 2: List of unused airport
	public static void runJob2(ArrayList<String> inFiles) {	
		//-------------------------------------------------------------------
		// Create a job for objective (a) Unused airports
		//-------------------------------------------------------------------
		Config config = new Config(inFiles, UnusedAirportMapper.class, UnusedAirportReducer.class);
		Job jobUnusedAirports = new Job(config);
		jobUnusedAirports.addRefData("AirportInfo", ((HashMap)dictAirportInfo).clone());
		try {
			jobUnusedAirports.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		displayUnusedAirportsAnalysis(jobUnusedAirports.getJobResult());  
	}
	
	public static void runJob3(ArrayList<String> inFiles) {
		//-------------------------------------------------------------------
		// Create a job for objective (b): List of flights
		//-------------------------------------------------------------------
		Config config = new Config(inFiles, FlightPassengerMapper.class, FlightPassengerReducer.class);
		Job jobFlightPassengers = new Job(config);
		try {
			jobFlightPassengers.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		displayFlightPassengersAnalysis(jobFlightPassengers.getJobResult());

	}
	
	public static void runJob4(ArrayList<String> inFiles) {
		//-------------------------------------------------------------------
		// Create a job for objective(d) Passenger having earned the highest air miles
		//-------------------------------------------------------------------
		Config config4 = new Config(inFiles, PassengerMileageMapper.class, PassengerMileageReducer.class);
		Job jobPassengerMileage = new Job(config4);
		jobPassengerMileage.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		try {
			jobPassengerMileage.addJobResultBucket("FlightMileage");
			jobPassengerMileage.addJobResultBucket("PassengerMileage");	
			jobPassengerMileage.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		displayPassengerMileageAnalysis(jobPassengerMileage.getJobResult("FlightMileage"), jobPassengerMileage.getJobResult("PassengerMileage"));
	}
	
	public static String getAirportName(String sCode) {
		if (dictAirportInfo!=null) {
			AirportInfo objAI = (AirportInfo)dictAirportInfo.get(sCode);
			return objAI.getName();
		} else
			return "";
	}

}
