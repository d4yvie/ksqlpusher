package org.sql.pusher.timescale;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.Batcher;
import org.sql.pusher.ForkJoinExecutor;
import org.sql.pusher.SqlPusher;

public class BulkTimeScalePusher implements SqlPusher {

	private final Connection connection = TimeScaleClientFactory.getConnection();

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		long t1 = System.nanoTime();
		System.out.println("STARTING BULK TIMESCALE INSERTS");
		String queryTemplate = String.format("INSERT INTO %s (Time, Amount, Fraud_check) VALUES ", stream);
		List<String> values = recordsStream
				.map(record -> String.format("(%s, %s, %s)", record.get(0), record.get(29), record.get(30)))
				.collect(Collectors.toList());
		Stream<List<String>> batches = Batcher.ofSubLists(values, 500);
		batches.forEach(batch -> {
			String concatenatedBatch = batch.stream().reduce("",
					(a, b) -> a.isEmpty() ? b : String.format("%s, %s", a, b));
			String query = queryTemplate + concatenatedBatch + ";";
			try (var stmt = connection.createStatement()) {
				stmt.execute(query);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		System.out.println(String.format("Success! Bulk TimeScale Inserting took %d seconds",
				TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
	}
}
