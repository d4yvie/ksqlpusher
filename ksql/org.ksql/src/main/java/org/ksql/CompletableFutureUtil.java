package org.ksql;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * @author ddy
 */
public class CompletableFutureUtil  {

    private CompletableFutureUtil(){}

    /**
     * Allows awaiting multiple CompletableFutures via Java's streaming API. Usage:
     * 	myStream.collect(CompletableFutureUtil.collectResult());
     * Transforms a <pre>{@code List<CompletableFuture<T>>}</pre> into a <pre>{@code CompletableFuture<List<T>>}</pre>
     * @param <X> the computed result type
     * @param <T> some CompletableFuture
     * @return a CompletableFuture of <pre>{@code CompletableFuture<List<T>>}</pre> that is complete when all collected CompletableFutures are complete.
     */
    public static <X, T extends CompletableFuture<X>> Collector<T, ?, CompletableFuture<List<X>>> collectResult(){
        return Collectors.collectingAndThen(Collectors.toList(), joinResult());
    }

    /**
     * Transforms a <pre>{@code List<CompletableFuture<?>>}</pre> into a <pre>{@code CompletableFuture<Void>}</pre>
     * Use this function if you are not interested in the collected results or the collected CompletableFutures are of
     * type Void.
     * @param <T> some CompletableFuture
     * @return a <pre>{@code CompletableFuture<Void>}</pre> that is complete when all collected CompletableFutures are complete.
     */
    public static <T extends CompletableFuture<?>> Collector<T, ?, CompletableFuture<Void>> allComplete(){
        return Collectors.collectingAndThen(Collectors.toList(), CompletableFutureUtil::allOf);
    }

    private static <X, T extends CompletableFuture<X>> Function<List<T>, CompletableFuture<List<X>>> joinResult() {
        return ls-> allOf(ls)
                .thenApply(v -> ls
                        .stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    private static <T extends CompletableFuture<?>> CompletableFuture<Void> allOf(List<T> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }
}