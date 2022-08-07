package org.ksql;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.sql.pusher.Batcher;


public class BatcherTest {
	
	@Test
	public void testSize2Batches() {
		List<String> testArr = List.of("Test1", "Test2", "Test3", "Test4");
		List<List<String>> result = Batcher.ofSize(testArr, 2);
		Assert.assertEquals("Test1", result.get(0).get(0));
		Assert.assertEquals("Test2", result.get(0).get(1));
		Assert.assertEquals("Test3", result.get(1).get(0));
		Assert.assertEquals("Test4", result.get(1).get(1));
	}

	@Test
	public void testSize3Batches() {
		List<String> testArr = List.of("Test1", "Test2", "Test3");
		List<List<String>> result = Batcher.ofSize(testArr, 3);
		Assert.assertEquals("Test1", result.get(0).get(0));
		Assert.assertEquals("Test2", result.get(0).get(1));
		Assert.assertEquals("Test3", result.get(0).get(2));
	}
}
