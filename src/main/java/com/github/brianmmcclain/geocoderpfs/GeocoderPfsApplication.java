package com.github.brianmmcclain.geocoderpfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import com.google.gson.Gson;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GeocoderPfsApplication {

	@Bean
	public Function<String, String> geocode() {
		return eventJson -> {

			// Set environment properties
			System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
			System.setProperty("java.net.preferIPv4Stack", "true");
			System.setProperty("javax.net.debug", "all");

			try {
			// Ensure we can connect to the Maps API
			String url = "https://maps.google.com";
		
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();

			// optional default is GET
			con.setRequestMethod("GET");

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(
					new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			//print result
			System.out.println(response.toString());
			} catch (Exception e) {
				String ret = "Could not connect to Maps API: " + e.getMessage();
				System.out.println(ret);
				e.printStackTrace(System.out);
				return ret;
			}

			// Parse environment variables
			String dbHostEnv = System.getenv("PGHOST");
			String dbPortEnv = System.getenv("PGPORT");
			String dbDatabaseEnv = System.getenv("PGDATABASE");
			String dbUserEnv = System.getenv("PGUSER");
			String dbPasswordEnv = System.getenv("PGPASSWORD");
			String googleAPIKey = System.getenv("GOOGLE_API_KEY");

			// Process defaults of environment variable isn't defined
			String dbHost = (dbHostEnv == null) ? "localhost" : dbHostEnv;
			String dbPort = (dbPortEnv == null) ? "5432" : dbPortEnv;
			String dbDatabase = (dbDatabaseEnv == null) ? "geocode" : dbDatabaseEnv;
			String dbUser = (dbUserEnv == null) ? "postgres" : dbUserEnv;
			String dbPassword = (dbPasswordEnv == null) ? "password" : dbPasswordEnv;

			// Connect to Postgres
			String dbString = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbDatabase);
			Properties props = new Properties();
			props.setProperty("user", dbUser);
			props.setProperty("password", dbPassword);
			Connection conn =  null;
			try {
				conn = DriverManager.getConnection(dbString, props);
			} catch (SQLException e) {
				// TODO: Process error
			}
			
			// Create table if it doesn't exist
			try {
				Statement createTableStatement = conn.createStatement();
				String createTableSql = "CREATE TABLE IF NOT EXISTS events (" +
					"id varchar(20) NOT NULL PRIMARY KEY," +
					"timestamp timestamp," +
					"lat double precision," +
					"lon double precision," +
					"mag real," +
					"address text" +
				");";
				
				createTableStatement.execute(createTableSql);
			} catch (SQLException e) {
				// TODO: Process error
				return "Exception: Could not create table";
			}

			// Parse event JSON
			Gson gson = new Gson();
			Map<String, String> event = gson.fromJson(eventJson, Map.class);
			String id = event.get("id");
			String time = event.get("timestamp");
			Double lat = Double.parseDouble(event.get("lat"));
			Double lon = Double.parseDouble(event.get("long"));
			Double mag = Double.parseDouble(event.get("mag"));

			// Parse timestamp
			Timestamp timestamp;
			try {
				//2018-12-06T15:39:26.928+0000
				SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
				Date parsedDate = timeFormat.parse(time);
				timestamp = new Timestamp(parsedDate.getTime());
			} catch (ParseException e) {
				// TODO: Process error
				return "Could not parse event: " + e.getMessage();
			}

			// Reverse geocode coordinates
			String address = reverseGeocode(lat, lon, googleAPIKey);

			// Write event to database
			try {
				PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)");
				insertStatement.setString(1, id);
				insertStatement.setTimestamp(2, timestamp);
				insertStatement.setDouble(3, lat);
				insertStatement.setDouble(4, lon);
				insertStatement.setDouble(5, mag);
				insertStatement.setString(6, address);
				insertStatement.execute();
				insertStatement.close();
			} catch (SQLException e) {
				// TODO: Process error
				return "Exception: Could not record event: " + e.getMessage();
			}

			return "Event " + id + " Recorded";
		};
	}

	private static String reverseGeocode(double lat, double lon, String apiKey) {
		try {
			GeoApiContext context = new GeoApiContext.Builder().apiKey(apiKey).build();
			LatLng location = new LatLng(lat, lon);
			GeocodingResult[] result = GeocodingApi.reverseGeocode(context, location).await();
			return result[0].formattedAddress;
		} catch (Exception e) {
			System.out.println("Error geocoding coordinates: " + e.getMessage() + ": " + lat + "," + lon);
			System.out.println(e.getClass().toString());
			System.out.println(e.getStackTrace().toString());
			return null;
		}
		
	}

	public static void main(String[] args) {
		SpringApplication.run(GeocoderPfsApplication.class, args);
	}
}
