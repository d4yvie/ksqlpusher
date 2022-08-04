package org.sql.pusher.timescale;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.Batcher;
import org.sql.pusher.SqlPusher;

public class BulkTimeScalePusher implements SqlPusher {

	private final Connection connection = TimeScaleClientFactory.getConnection();

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		long t1 = System.nanoTime();
		long t1_transform = System.nanoTime();
		System.out.println("STARTING BULK TIMESCALE INSERTS");
		String queryTemplate = String.format("INSERT INTO %s (Entrynumber, time, cc_num, merchant, category, amt, firstname , lastname , gender , street , city , state , zip , lat , long , city_pop , job , trans_num , unix_time , merch_lat , merch_long , is_fraud ) VALUES ", stream);
		Stream<Object[]> result = recordsStream.map(val -> val.toList())
				.map(val -> val.toArray(new String[22])).map(arr -> {
					// transform text-columns
					Stream.of(1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 16 ,17)
						.forEach(val -> {
							arr[val] = toSQLString(arr[val]);
						});
					return arr;
				});
		String valueTemplate = createRowTemplate(21);
		List<String> values = result
				.map(record -> String.format(valueTemplate, record))
				.collect(Collectors.toList());
		long transformTime = System.nanoTime() - t1_transform;
		System.out.println(String.format("Transforming took %d seconds",
				TimeUnit.SECONDS.convert(transformTime, TimeUnit.NANOSECONDS)));
		Stream<List<String>> batches = Batcher.ofSubLists(values, 500);
		batches.forEach(batch -> {
			String concatenatedBatch = batch.stream().reduce(new StringBuilder(),
					(a, b) -> a.toString().isEmpty() ? a.append(b) : a.append(",").append(b), (a, b) -> b).toString();
			String query = queryTemplate + concatenatedBatch + ";";
			try (var stmt = connection.createStatement()) {
				stmt.execute(query);
			} catch (Exception e) {
				System.out.println(query);
				e.printStackTrace();
			}
		});
		long fullInsertTime = System.nanoTime() - t1;
		System.out.println(String.format("Success! Bulk TimeScale Inserting took %d seconds",
				TimeUnit.SECONDS.convert(fullInsertTime, TimeUnit.NANOSECONDS)));
		System.out.println(String.format("Success! Bulk TimeScale Inserting without transform took %d seconds",
				TimeUnit.SECONDS.convert(fullInsertTime - transformTime, TimeUnit.NANOSECONDS)));
	}

	private String toSQLString(String val) {
		return "'" + val.replace("'", "''") + "'";
	}

	private String createRowTemplate(int columns) {
		return IntStream.range(0, columns).mapToObj(it -> "")
				.reduce("(%s", (acc, it) -> acc + ", %s") + ")";
	}

	public static void main(String[] args) {
		System.out.println(IntStream.range(0, 20).mapToObj(it -> "")
				.reduce("(%s", (acc, it) -> acc + ", %s")+ ")") ;
	}
}
