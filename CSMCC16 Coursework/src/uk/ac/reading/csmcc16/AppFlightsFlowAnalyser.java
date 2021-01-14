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
import java.text.DecimalFormat;
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
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Callback;
import javafx.scene.Cursor;
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
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
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
import javafx.scene.paint.Color;
import javafx.scene.shape.Box;
import javafx.scene.shape.CullFace;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import javafx.scene.transform.Rotate;

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
	static Map<String, Object> dictAirportInfo = new HashMap<String, Object>();
	
	// JavaFX objects
	TextField txtFldAirportDataFile;
	TextField txtFldPassengerDataFile;
	static TableView tblVwAirportFlights;
	static TableView tblVwUnusedAirports;
	static TableView tblVwFlightPassengers;
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

			primaryStage.setScene(new Scene(new VBox(tbPnMain), 800, 600));
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
		tblVwAirportFlights.setMinWidth(400);


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

		tblVwFlightPassengers = new TableView();

		// Set the table view to resize columns automatically
		tblVwFlightPassengers.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);

		// Use Map object to add data		
		TableColumn<Map, String> column_a1 = new TableColumn<>("Flight ID");
		column_a1.setCellValueFactory(new MapValueFactory<>("flightID"));

		TableColumn<Map, String> column_a2 = new TableColumn<>("Number of Passengers");
		column_a2.setCellValueFactory(new MapValueFactory<>("numOfPassengers"));
		column_a2.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a3 = new TableColumn<>("Airport From");
		column_a3.setCellValueFactory(new MapValueFactory<>("airportFrom"));
		column_a3.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a4 = new TableColumn<>("Airport To");
		column_a4.setCellValueFactory(new MapValueFactory<>("airportTo"));
		column_a4.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a5 = new TableColumn<>("Flight Mileage");
		column_a5.setCellValueFactory(new MapValueFactory<>("flightMileage"));	
		column_a5.setStyle("-fx-alignment: CENTER_RIGHT;");

		TableColumn<Map, String> column_a6 = new TableColumn<>("Departure Time");
		column_a6.setCellValueFactory(new MapValueFactory<>("departureTime"));
		column_a6.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a7 = new TableColumn<>("Arrival Time");
		column_a7.setCellValueFactory(new MapValueFactory<>("arrivalTime"));
		column_a7.setStyle("-fx-alignment: CENTER;");

		TableColumn<Map, String> column_a8 = new TableColumn<>("Flight Time");
		column_a8.setCellValueFactory(new MapValueFactory<>("flightTime"));	
		column_a8.setStyle("-fx-alignment: CENTER_RIGHT;");
		
		tblVwFlightPassengers.getColumns().add(column_a1);
		tblVwFlightPassengers.getColumns().add(column_a2);
		tblVwFlightPassengers.getColumns().add(column_a3);
		tblVwFlightPassengers.getColumns().add(column_a4);
		tblVwFlightPassengers.getColumns().add(column_a5);
		tblVwFlightPassengers.getColumns().add(column_a6);
		tblVwFlightPassengers.getColumns().add(column_a7);
		tblVwFlightPassengers.getColumns().add(column_a8);

		return (new VBox(tblVwFlightPassengers));
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
        Iterator iterator = mapResult.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
            FlightPassengerInfo objFP = (FlightPassengerInfo)entry.getValue();
            List passengers = objFP.getPassengers();
			Map<String, Object> item = new HashMap<>();
			item.put("flightID", entry.getKey());
			item.put("numOfPassengers" , objFP.getNumOfPassengers());
			item.put("airportFrom", objFP.getAirportFrom());
			item.put("airportFromName", getAirportName(objFP.getAirportFrom()));
			item.put("airportTo", objFP.getAirportTo());
			item.put("airportToName", getAirportName(objFP.getAirportTo()));
			item.put("flightMileage", new DecimalFormat("#,###").format(objFP.getFlightMileage()));
			item.put("departureTime", objFP.getDepTime());
			item.put("arrivalTime", objFP.getArrTime());
			item.put("flightTime", objFP.getFlightTime());
			items.add(item);
        } 
		tblVwFlightPassengers.getItems().addAll(items);
		Utilities.autoResizeTableViewColumns(tblVwFlightPassengers);
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
        //autoResizeTreeTableViewColumns(trTblVwPassengerMileage);
        
	}
	
	public static void startMapReduceJobs(String sAirportDataFile, String sPassengerDataFile) {
		
		// Set mouse pointer to Cursor.WAIT
		Utilities.setCursorWait(stgPrimaryStage.getScene());
		
		// Load the Airport data from file
		dictAirportInfo = Utilities.loadAirportData(sAirportDataFile);		
		
		// Split the input file into files with smaller partitions with defined size in properties file
		File inputFile = new File(sPassengerDataFile);
		
		List<File> splitFiles = null;
		
		try {	
			splitFiles = Utilities.splitFile(inputFile, Integer.parseInt(configProps.getProperty("partition.size")));
		} catch (Exception e) {
			Logger.getInstance().logError("Error in splitting the input file: " + e);		
			return;
		}
		
		// Assign each split file to a mapper instance to run concurrently
		ArrayList<String> inFiles = new ArrayList<String>();
		for (File sf: splitFiles) {
			System.out.println("Filename:" + sf.getName());
			inFiles.add(sf.getName());
		}
		
		ArrayList<String> inFilesJob2 = new ArrayList<String>();
		inFilesJob2.add(configProps.getProperty("job1.outputfile.2"));
		
		// Run Job 1 for objective (a): No. of flights from each airport
		runJob1(inFiles);
		
		// Run Job 2 for objective (a): Unused airports
		runJob2(inFilesJob2);
		
		// Run Job 3 for objective (b): List of flights with passenger list, 
		// (c): No. of passengers on each flight, and (d): Line-of-sight miles for each flight
		runJob3(inFiles);
		
		// Run Job 4 for objective (d) Total mileage traveled by each passenger, and passenger having highest mileage
		runJob4(inFiles);
		
		// Set mouse pointer back to Cursor.DEFAULT
		Utilities.setCursorDefault(stgPrimaryStage.getScene());
	}
	
	// Job 1: Flights from each airport
	public static void runJob1(ArrayList<String> inFiles) {
		//-------------------------------------------------------------------
		// Create a job for objective (a)
		//-------------------------------------------------------------------
		Config config = new Config(inFiles, AirportFlightMapper.class, AirportFlightReducer.class, false);
		// add the airport data into the config

		Job jobAirportFlights = new Job(config);
		try {
			jobAirportFlights.addJobResultBucket("UsedAirports");
			jobAirportFlights.addJobResultBucket("FlightCount");			
			jobAirportFlights.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Write output file #1
		// Write the list of used airports to output file for job chaining
        Iterator iterator1 = jobAirportFlights.getJobResult("FlightCount").entrySet().iterator();
        
        PrintWriter writer1 = null;
		try {
			writer1 = new PrintWriter(new File(configProps.getProperty("job1.outputfile.1")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Print column headers into the output file
		writer1.println("AirportCode,AirportName,NumOfFlights");
	    while(iterator1.hasNext()) {
	            Map.Entry<Object, Integer> entry = (Map.Entry) iterator1.next();
	            writer1.println(entry.getKey() + "," + getAirportName((String)entry.getKey()) + "," + entry.getValue());
	        } 
        writer1.close();
        
        
		// Write output file #2
		// Write the list of used airports to output file for job chaining
        Iterator iterator2 = jobAirportFlights.getJobResult("UsedAirports").entrySet().iterator();
        
        PrintWriter writer2 = null;
		try {
			writer2 = new PrintWriter(new File(configProps.getProperty("job1.outputfile.2")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    while(iterator2.hasNext()) {
	            Map.Entry<Object, FlightTripInfo> entry = (Map.Entry) iterator2.next();
	            FlightTripInfo objFTI = entry.getValue();
	            // Write entry to the output file
	            writer2.println(objFTI.getAirportFrom() + "," + objFTI.getAirportTo());
	        } 
        writer2.close();
        
		// Output for the objective (a)
		displayAirportsAnalysis(jobAirportFlights.getJobResult("FlightCount"));
		
		
	}
	
	
	// Job 2: List of unused airport
	public static void runJob2(ArrayList<String> inFiles) {	
		//-------------------------------------------------------------------
		// Create a job for objective (a) Unused airports
		//-------------------------------------------------------------------
		Config config = new Config(inFiles, UnusedAirportMapper.class, UnusedAirportReducer.class, true);
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
		Config config = new Config(inFiles, FlightPassengerMapper.class, FlightPassengerReducer.class, false);
		Job jobFlightPassengers = new Job(config);
		jobFlightPassengers.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		
		try {
			jobFlightPassengers.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		displayFlightsAnalysis(jobFlightPassengers.getJobResult());

	}
	
	public static void runJob4(ArrayList<String> inFiles) {
		//-------------------------------------------------------------------
		// Create a job for objective(d) Passenger having earned the highest air miles
		//-------------------------------------------------------------------
		Config config = new Config(inFiles, PassengerMileageMapper.class, PassengerMileageReducer.class, false);
		Job jobPassengerMileage = new Job(config);
		jobPassengerMileage.addRefData("AirportInfo", ((HashMap) dictAirportInfo).clone());
		try {
			jobPassengerMileage.addJobResultBucket("FlightMileage");
			jobPassengerMileage.addJobResultBucket("PassengerMileage");	
			jobPassengerMileage.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		displayPassengersAnalysis(jobPassengerMileage.getJobResult("FlightMileage"), jobPassengerMileage.getJobResult("PassengerMileage"));
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
