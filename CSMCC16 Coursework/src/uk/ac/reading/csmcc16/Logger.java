package uk.ac.reading.csmcc16;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javafx.scene.control.TextArea;

public class Logger {
	
    private TextArea textArea;
    private static Logger _logger = null;
    private boolean verboseMode = false;
    private boolean debugMode = false;

    private Logger() {}
    
    public static Logger getInstance() {
    	if (_logger==null)
    		_logger = new Logger();
    	return _logger;
    }
    public void setTextArea(TextArea textArea) {
        this.textArea = textArea;
        this.textArea.setEditable(false);
    }

    public void setVerboseMode(boolean value) {
        verboseMode = value;
    }

    public void setDebugMode(boolean value) {
        debugMode = value;
    }

    public boolean writeMessage(String msg) {
    	if (textArea!=null) {
    		textArea.appendText(msg);
    		return true;
    	} else {
    		System.err.println(getTimeStamp() + "Logger: TextArea not initialised.");
    		return false;
    	}
    }

    public boolean logMessage(String msg) {
        return writeMessage(getTimeStamp() + msg + "\n");
    }

    public boolean logWarning(String msg) {
        return writeMessage(getTimeStamp() + "[WARNING] " + msg + "\n");
    }

    public boolean logError(String msg) {
        return writeMessage(getTimeStamp() + "[ERROR] " + msg + "\n");
    }

    public boolean logVerbose(String msg) {
        return verboseMode ? writeMessage(getTimeStamp() + msg + "\n") : true;
    }

    public boolean logDebug(String msg) {
        return debugMode ? writeMessage(getTimeStamp() + "[DEBUG] " + msg + "\n") : true;
    }
    
    private String getTimeStamp() {
    	return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS  "));
    }
}