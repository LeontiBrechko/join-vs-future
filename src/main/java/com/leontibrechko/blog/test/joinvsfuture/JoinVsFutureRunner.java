package com.leontibrechko.blog.test.joinvsfuture;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

@Slf4j
@SpringBootApplication
public class JoinVsFutureRunner {
    private static final SfnAsyncClient SFN_ASYNC_CLIENT_1 = createSfnAsyncClient();
    // Test with 1 SDK Client
    // private static final SfnAsyncClient SFN_ASYNC_CLIENT_2 = SFN_ASYNC_CLIENT_1;
    // Test with 2 SDK Clients
    private static final SfnAsyncClient SFN_ASYNC_CLIENT_2 = createSfnAsyncClient();

    private static final int NUMBER_OF_TEST_RUNS = 20;
    private static final int NUMBER_OF_INVOCATIONS_PER_TEST = 1000;
    private static final Duration[] TEST_DURATIONS = new Duration[NUMBER_OF_TEST_RUNS];

    public static void main(String[] args) throws Exception {
        SpringApplication.run(JoinVsFutureRunner.class, args);

        // Exceptionally Compose
        // final Supplier<CompletableFuture<Void>> testCallingMethod = JoinVsFutureRunner::runWithExceptionallyCompose;
        // .join() Test
        // final Supplier<CompletableFuture<Void>> testCallingMethod = JoinVsFutureRunner::runWithJoin;
        // Separate CompletableFuture
        final Supplier<CompletableFuture<Void>> testCallingMethod = JoinVsFutureRunner::runWithSeparateFuture;

        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            TEST_DURATIONS[i] = runTest(testCallingMethod);
        }

        final LongSummaryStatistics overallDurationStatisticsInMs = Arrays.stream(TEST_DURATIONS)
                .mapToLong(Duration::toMillis)
                .summaryStatistics();

        log.info("Test statistics:");
        log.info("Total test runs: {}", overallDurationStatisticsInMs.getCount());
        log.info("Total run time (ms): {}", overallDurationStatisticsInMs.getSum());
        log.info("Average (ms): {}", overallDurationStatisticsInMs.getAverage());
        log.info("Max (ms): {}", overallDurationStatisticsInMs.getMax());
        log.info("Min (ms): {}", overallDurationStatisticsInMs.getMin());
    }

    private static Duration runTest(final Supplier<CompletableFuture<Void>> testInvocationMethod) throws Exception {
        final long startTime = System.nanoTime();

        final CompletableFuture<Void>[] invocationFutures = new CompletableFuture[NUMBER_OF_INVOCATIONS_PER_TEST];
        for (int i = 0; i < NUMBER_OF_INVOCATIONS_PER_TEST; i++) {
            invocationFutures[i] = testInvocationMethod.get()
                    .exceptionally(finalException -> null);
        }

        CompletableFuture.allOf(invocationFutures).get();

        return Duration.ofNanos(System.nanoTime() - startTime);
    }

    private static CompletableFuture<Void> runWithExceptionallyCompose() {
        return makeFirstAsyncRequest()
                .exceptionallyCompose(exception -> {
                    return makeSecondAsyncRequest()
                            .thenAccept(voidResult -> {
                                throw new CompletionException(exception);
                            });
                });
    }

    private static CompletableFuture<Void> runWithJoin() {
        return makeFirstAsyncRequest()
                .exceptionally(exception -> {
                    makeSecondAsyncRequest().join();
                    throw new CompletionException(exception);
                });
    }

    private static CompletableFuture<Void> runWithSeparateFuture() {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        makeFirstAsyncRequest()
                .exceptionally(exception -> {
                    makeSecondAsyncRequest()
                            .whenComplete((voidResult, innerException) -> {
                                if (innerException != null) resultFuture.completeExceptionally(innerException);
                                else resultFuture.completeExceptionally(exception);
                            });

                    return null;
                });

        return resultFuture;
    }

    private static CompletableFuture<Void> makeFirstAsyncRequest() {
        return asyncCall(SFN_ASYNC_CLIENT_1);
    }

    private static CompletableFuture<Void> makeSecondAsyncRequest() {
        return asyncCall(SFN_ASYNC_CLIENT_2);
    }

    private static CompletableFuture<Void> asyncCall(final SfnAsyncClient client) {
        return client.listStateMachines()
                .thenApply(listStateMachinesResponse -> null);
    }

    private static SfnAsyncClient createSfnAsyncClient() {
        return SfnAsyncClient.builder()
                // invalid, but DNS-resolvable, endpoint to cause a connection timeout error
                // this will block an async invocation by the time specified in "connectionTimeout"
                .endpointOverride(URI.create("http://amazon.com"))
                .httpClientBuilder(
                        NettyNioAsyncHttpClient.builder()
                                .connectionTimeout(Duration.ofMillis(10))
                )
                .build();
    }
}
