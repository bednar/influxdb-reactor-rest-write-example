package com.github.bednar.influxdbreactorrestwriteexample;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Jakub Bednar (31/05/2021 10:50)
 */
@RestController
public class WriterController {

    private static final Logger logger = LoggerFactory.getLogger(WriterController.class);

    private static final int WRITE_BATCH_SIZE = 2;
    private static final String BUCKET = "my-bucket";
    private static final String ORG = "my-org";
    private static final WritePrecision PRECISION = WritePrecision.MS;
    public static final int MAX_RETRY_ATTEMPS = 3;
    public static final Duration MIN_BACKOFF_TIME = Duration.ofMillis(100);

    private final WriteApiBlocking writeClient;

    public WriterController(InfluxDBClient influxDBClient) {
        this.writeClient = influxDBClient.getWriteApiBlocking();
    }

    @GetMapping("/writeDemoData")
    Mono<Void> writeDemoData() {
        String observerId = observerId();
        Map<String, Object> tags = tags();
        Table<Instant, String, Double> values = values();

        return Flux.fromIterable(values.rowMap().entrySet())
                // create Point
                .map(cell -> Point.measurement(observerId)
                        .addTags(serializeTags(tags))
                        .time(cell.getKey(), PRECISION)
                        .addFields(Collections.unmodifiableMap(cell.getValue()))
                        .toLineProtocol())
                // create Batch
                .buffer(WRITE_BATCH_SIZE)
                // write Batch
                .flatMap(records -> Mono
                        .fromRunnable(() -> {
                            writeClient.writeRecords(BUCKET, ORG, PRECISION, records);
                            logger.info("Wrote {} points to bucket {} for observer {} with tags {}", records.size(), BUCKET, observerId, tags);
                        })
                )
                .retryWhen(RetryBackoffSpec
                        .backoff(MAX_RETRY_ATTEMPS, MIN_BACKOFF_TIME)
                        .doAfterRetry(retrySignal -> logger.info("Retry due the exception: {}", retrySignal.failure().getMessage())))
                .then();
    }

    @NonNull
    private String observerId() {
        return UUID.randomUUID().toString();
    }

    @NonNull
    private Map<String, Object> tags() {
        Map<String, Object> tags = new HashMap<>();
        tags.put("tag-a", "a");
        tags.put("tag-b", "b");
        return tags;
    }

    @NonNull
    private Table<Instant, String, Double> values() {

        Instant now = Instant.now();
        Table<Instant, String, Double> table = HashBasedTable.create();
        
        // record 1
        table.put(now, "field-a", 30D);
        table.put(now, "field-b", 30D);
        // record 2
        table.put(now.plusMillis(1), "field-a", 60D);
        table.put(now.plusMillis(1), "field-b", 60D);
        // record 3
        table.put(now.plusMillis(2), "field-a", 90D);
        table.put(now.plusMillis(2), "field-b", 90D);
        // record 4
        table.put(now.plusMillis(3), "field-a", 120D);
        table.put(now.plusMillis(3), "field-b", 120D);

        return table;
    }

    @NonNull
    private Map<String, String> serializeTags(@NonNull Map<String, Object> tags) {
        return tags.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().toString()));
    }
}

