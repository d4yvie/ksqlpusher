package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

public class Main {

	public static String KSQLDB_SERVER_HOST = "localhost";
	public static int KSQLDB_SERVER_HOST_PORT = 8088;

	public static void main(String[] args) throws IOException {
		new Main().doIt();
	}

	public void doIt() throws IOException {
		ClientOptions options = ClientOptions.create().setHost(KSQLDB_SERVER_HOST).setPort(KSQLDB_SERVER_HOST_PORT);
		Client client = Client.create(options);
		Reader in = new FileReader("src/main/resources/my.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		StreamSupport.stream(records.spliterator(), false)
			.forEach(this::handleRecord);
		// Send requests with the client by following the other examples
		
		// Terminate any open connections and close the client
		client.close();
	}
	
	public void handleRecord(CSVRecord record) {
		String columnOne = record.get(0);
	    String columnTwo = record.get(1);
	    String columnThree = record.get(2);
	    String columnFour = record.get(3);
	    System.out.println(String.format("%s %s %s %s", columnOne, columnTwo, columnThree, columnFour));
	}
}
