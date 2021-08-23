package com.github.bednar.influxdbreactorrestwriteexample;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.influxdb.client.reactive.WriteOptionsReactive;
import com.influxdb.client.write.Point;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class SpringReactorInfluxDbClientWriteExampleApplicationTests {
	private static final Logger logger = LoggerFactory.getLogger(WriterController.class);

	@Test
	void testWriteDemo(@Autowired WriterController writerController) {
		StepVerifier.create(writerController.writeDemoData())
				.expectNext()
				.expectComplete()
				.verify();
	}

	@Test
	void writeSuccess(@Autowired WriterController writerController) throws InterruptedException {
		ThreadPoolExecutor executor =
				(ThreadPoolExecutor) Executors.newFixedThreadPool(30);

		for (int i = 0; i < 1000; i++) {
			executor.submit(() -> {
				writerController.writeDemoData().subscribe(unused -> {
				}, throwable -> {
					logger.error("Error in write", throwable);
				});
			});
		}
		sleep(10000);
		executor.shutdownNow();
		Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
	}

	@Test
	void writeSuccessPassData(@Autowired InfluxDBClientReactive client,
							  @Autowired WriterController writerController) {

		int rows_count = 10_000;
		int columns_count = 20;
		int tags_count = 10;
		String measurement = "testObserver_" + System.currentTimeMillis();

		// prepare data
		Table<Instant, String, Double> out = generateData(rows_count, columns_count);
		Map<String, Object> tags = generateTags(tags_count);

		// write data
		writerController
				.writeDemoData(measurement, tags, out)
				.block();

		// prepare query
		String query = String.format("from(bucket: \"%s\")\n" +
				"|> range(start: 0)\n" +
				"|> filter(fn: (r) => r[\"_measurement\"] == \"%s\")\n" +
				"|> count()", BUCKET, measurement);

		Long calculatedCount = Mono.just(query)
				.flatMapMany(it -> client.getQueryReactiveApi().query(it))
				.mapNotNull(fluxRecord -> (Long) fluxRecord.getValue())
				.reduce(Long::sum)
				.block();

		assertEquals(rows_count * columns_count, calculatedCount);
	}

	private static final String BUCKET = "my-bucket";
    private static final String URL = "http://localhost:8086";
    private static final String TOKEN = "my-token";
    private static final String ORG = "my-org";
    
    @Test
    public void largerThanABatch() {
        // Create Influx Reactive Client
        InfluxDBClientReactive influxDBClientReactive = InfluxDBClientReactiveFactory.create(
                InfluxDBClientOptions.builder()
                        .url(URL)
                        .authenticateToken(TOKEN.toCharArray())
                        .org(ORG)
                        .build()
        );

        // create write options with a batch size of 10
        int WRITE_BATCH_SIZE = 10;
        WriteOptionsReactive writeOptions = WriteOptionsReactive.builder()
                .batchSize(WRITE_BATCH_SIZE)
                .build();

        // Create 2 batches worth of data
        int iterations = 2;
        Instant now = Instant.now();
        String observer = "testObserver_" + System.currentTimeMillis();
        Table<Instant, String, Double> out = HashBasedTable.create(WRITE_BATCH_SIZE * iterations, 1);
        for (int i = 0; i < WRITE_BATCH_SIZE * iterations; i++) {
            out.put(now.minusMillis(i + 1), "a", ThreadLocalRandom.current().nextDouble(0.0, 10.0));
        }


		// ~~~~~~~~~~~~~~~~~~~ WRITE POINTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Write 2 batches worth of data at once using writePoints
        Flux.fromIterable(out.rowMap().entrySet())
                // generate the points going to the writePoints
                .map(cell -> Point.measurement(observer)
                        .addTags(Maps.newHashMap())
                        .time(cell.getKey(), WritePrecision.MS)
                        .addFields(Collections.unmodifiableMap(cell.getValue())))
                .as(upstream -> Flux.from(influxDBClientReactive.getWriteReactiveApi(writeOptions)
                        .writePoints(BUCKET, ORG, WritePrecision.MS, upstream))) // Instead of `Mono.from` which takes the first emitted item
                                                                                 // you should use `Flux.from` + `reduce`.
                .doOnNext(a -> System.out.println(a.toString())) // should see 2 success objects emitted
                .reduce((success1, success2) -> success1)
                .block(); // all the points should have been written at this point


        // ~~~~~~~~~~~~~~~~~~~ COUNT POINTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // count how many observations the db sees
        Long count = Mono.fromCallable(() ->
                "from(bucket: \"" + BUCKET + "\")\n" +
                        "  |> range(start: 0)\n" +
                        "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + observer + "\")\n" +
                        "  |> filter(fn: (r) => r[\"_field\"] == \"a\")\n" +
                        "  |> count()\n" +
                        "  |> yield(name: \"count\")"
        )
                .flatMapMany(query -> influxDBClientReactive.getQueryReactiveApi().query(query))
                .mapNotNull(fluxRecord -> (Long) fluxRecord.getValue())
                .reduce(Long::sum)
                .block();

        // the number written to the db should equal the
        assertEquals(WRITE_BATCH_SIZE * iterations, count);
    }

	@NotNull
	private Table<Instant, String, Double> generateData(int rows, int columns) {
		Instant now = Instant.now();
		Table<Instant, String, Double> out = HashBasedTable.create(rows, 1);
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < columns; j++) {
				out.put(now.minusMillis(i + 1), "col-" + j, ThreadLocalRandom.current().nextDouble(0.0, 10.0));
			}
		}
		return out;
	}

	@NonNull
	private Map<String, Object> generateTags(int count) {
		Map<String, Object> tags = new HashMap<>();
		for (int i = 0; i < count; i++) {
			tags.put("tag-" + i, i);
		}
		return tags;
	}
}
