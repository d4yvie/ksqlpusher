package org.sql.pusher;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

public class KsqlClientFactory {
	
	private static final String KSQLDB_SERVER_HOST = "localhost";
	private static final int KSQLDB_SERVER_HOST_PORT = 8088;
	private static Client client = createClient();
	
	private static Client createClient() {
		ClientOptions options = ClientOptions.create().setHost(KSQLDB_SERVER_HOST).setPort(KSQLDB_SERVER_HOST_PORT);
		return Client.create(options);
	}
	
	public static Client retrieveClient() {
		return client;
	}
}
