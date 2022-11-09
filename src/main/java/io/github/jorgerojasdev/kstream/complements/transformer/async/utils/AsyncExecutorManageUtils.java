package io.github.jorgerojasdev.kstream.complements.transformer.async.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AsyncExecutorManageUtils {

	private static final Map<String, Executor> executors = new HashMap<>();

	public static synchronized Executor getOrCreateExecutor(String name, Integer parallelism) {
		if (executors.containsKey(name)) {
			return executors.get(name);
		}

		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(1);
		executor.setMaxPoolSize(parallelism);
		executor.setQueueCapacity(0);
		executor.setThreadNamePrefix(String.format("%s-async-worker-", name));
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		executor.initialize();

		executors.put(name, executor);

		return executor;
	}

}
