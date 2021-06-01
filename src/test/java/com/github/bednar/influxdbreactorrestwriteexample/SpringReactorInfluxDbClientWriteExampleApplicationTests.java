package com.github.bednar.influxdbreactorrestwriteexample;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

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
		sleep(5000);
		executor.shutdownNow();
		Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
	}

}
