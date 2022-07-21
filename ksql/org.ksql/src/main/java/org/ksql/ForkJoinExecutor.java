package org.ksql;

import java.util.concurrent.ForkJoinPool;

public class ForkJoinExecutor {

	private final ForkJoinPool forkJoinPool;
	
	public ForkJoinExecutor(int parallelism) {
		forkJoinPool = new ForkJoinPool(parallelism);
	}

	public void runInPool(Runnable toRun) {
	    try {
	    	if (forkJoinPool.getParallelism() == 1) {
	    		toRun.run();
	    	} else {
	    		forkJoinPool.submit(toRun).get();
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
