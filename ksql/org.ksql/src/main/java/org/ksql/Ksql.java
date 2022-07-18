package org.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

public class Ksql {
	
	public static String KSQLDB_SERVER_HOST = "localhost";
	public static int KSQLDB_SERVER_HOST_PORT = 8088;
	
	public static Client createClient() {
		ClientOptions options = ClientOptions.create().setHost(KSQLDB_SERVER_HOST).setPort(KSQLDB_SERVER_HOST_PORT);
		return Client.create(options);
	}

}
