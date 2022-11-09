package io.github.jorgerojasdev.kstream.complements.transformer.common.internals;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.StatefulTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractStatefulTransformer<K, V, R> implements StatefulTransformer<K, V, R> {

	protected AbstractProcessorContext context;

	private final Map<String, StateStore> stateStoreMap = new HashMap<>();

	protected abstract Set<StateStoreExecutor> stateStoreExecutors();

	@Override public void init(ProcessorContext context) {
		this.context = (AbstractProcessorContext) context;
		Arrays.asList(stateStoreNames()).forEach(stateStoreName ->
				stateStoreMap.put(stateStoreName, this.context.getStateStore(stateStoreName))
		);
		stateStoreExecutors().forEach(stateStoreExecutor ->
				stateStoreExecutor.cleaningStrategy().scheduleCleaningStrategy(stateStoreMap, this.context)
		);
	}

	@Override public final KeyValue<K, R> transform(K key, V value) {
		return transformStrategy().transform(stateStoreMap, ProcessorRecordInfo.<K, V>builder()
				.key(key)
				.value(value)
				.currentSystemTimestamp(context.currentSystemTimeMs())
				.onInsertTopicTimestamp(context.timestamp())
				.build());
	}

	@Override public final void close() {
		//Default behaviour on close
	}

	@Override public final void registerStateStores(StreamsBuilder streamsBuilder) {
		this.stateStoreExecutors().forEach(stateStoreExecutor -> {
			try {
				streamsBuilder.addStateStore(stateStoreExecutor.storeBuilder());
			} catch (Exception e) {
				String logMessage = String.format("Non-blocking exception: %s", e.getMessage());
				log.debug(logMessage, e);
			}
		});
	}

}
