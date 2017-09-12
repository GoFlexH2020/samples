package de.tud.ecast;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//jar from http://mvnrepository.com/artifact/org.json/json/20170516
import org.json.JSONObject;
import org.json.JSONTokener;

public class client {
	public static void main(String[] argv) {
		Properties prop = new Properties();
		try {
			InputStream configFile = new FileInputStream("config.properties");
			prop.load(configFile);
			configFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Read the template JSON for retrieving meter data
		InputStream jsonFile;
		try {
			jsonFile = new FileInputStream("goflex-meterdata.json");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}
			
		//Now connect to the messaging system
		GOFLEXAPI api = new GOFLEXAPI(prop.getProperty("host"),
				Integer.parseInt(prop.getProperty("port")),
				prop.getProperty("user"),
				prop.getProperty("password"),
				prop.getProperty("publish"),
				prop.getProperty("subscribe"));
		
		//Add the appropriate date range etc
		JSONTokener jsonTokener = new JSONTokener(jsonFile);
		JSONObject message = new JSONObject(jsonTokener);
		message.getJSONObject("serviceRequest").getJSONObject("service").getJSONObject("args").put("ts_id", 0);
		message.getJSONObject("serviceRequest").getJSONObject("service").getJSONObject("args").put("from", "2009-07-13T00:00:00+0000");
		message.getJSONObject("serviceRequest").getJSONObject("service").getJSONObject("args").put("to", "2017-07-14T01:00:00+0000");
		
		//And send our message
        int correlationId = 1;
        api.publish(message, correlationId);
        System.out.println("Sent: " + message);
        
        // Now wait for the reply
        System.out.println("Waiting for messages. To exit press CTRL+C");
        api.receive();
	}
}
