package io.github.jorgerojasdev.kstream.complements.transformer.async.internals.store;

import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecord;
import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecordStatus;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreCleaningStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import lombok.Builder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

@Builder
public class AsyncStateStoreExecutor<K, V, R> extends AbstractStateStoreExecutor {

	private final String stateStoreName;
	private final StateStoreDefinition<V> stateStoreDefinition;
	private final Function<V, Duration> timeoutFunction;
	private final BiFunction<K, V, KeyValue<K, R>> onTimeoutForward;

	@Override public StoreBuilder<?> storeBuilder() {
		return Stores.timestampedKeyValueStoreBuilder(
				Stores.persistentTimestampedKeyValueStore(stateStoreName),
				stateStoreDefinition.getKeySerde(),
				stateStoreDefinition.getValueSerde()
		).withLoggingEnabled(stateStoreDefinition.getChangelogProperties());
	}

	@SuppressWarnings("unchecked")
	@Override public StateStoreCleaningStrategy cleaningStrategy() {
		return (stateStoreMap, context) -> {
			TimestampedKeyValueStore<K, V> stateStore = (TimestampedKeyValueStore<K, V>) stateStoreMap.get(stateStoreName);

			return context.schedule(stateStoreDefinition.getCleaningAccuracy(), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
				try (KeyValueIterator<K, ValueAndTimestamp<V>> keyValueIterator = stateStore.all()) {
					while (keyValueIterator.hasNext()) {
						KeyValue<K, ValueAndTimestamp<V>> keyValue = keyValueIterator.next();
						long currentTimestamp = context.currentSystemTimeMs();

						if (hasExpiredRecord(currentTimestamp, keyValue.value.timestamp(), stateStoreDefinition.getMaxTimeOfSavedRecords())) {
							onExpiredMaxTimeRecord(stateStore, keyValue, context);
							continue;
						}

						if (hasExpiredRecord(currentTimestamp, keyValue.value.timestamp(), timeoutFunction.apply(keyValue.value.value()))) {
							onTimeout(stateStore, keyValue, context);
						}
					}
				}
			});
		};
	}

	private void onExpiredMaxTimeRecord(TimestampedKeyValueStore<K, V> stateStore, KeyValue<K, ValueAndTimestamp<V>> keyValue, ProcessorContext context) {
		KeyValue<K, R> transformedValue = onTimeoutForward.apply(keyValue.key, keyValue.value.value());

		stateStore.delete(keyValue.key);
		context.forward(transformedValue.key, AsyncRecord.<R>builder().status(AsyncRecordStatus.EXPIRED).value(transformedValue.value).build());
	}

	private void onTimeout(TimestampedKeyValueStore<K, V> stateStore, KeyValue<K, ValueAndTimestamp<V>> keyValue, ProcessorContext context) {
		KeyValue<K, R> transformedValue = onTimeoutForward.apply(keyValue.key, keyValue.value.value());

		stateStore.delete(keyValue.key);
		context.forward(transformedValue.key, AsyncRecord.<R>builder().status(AsyncRecordStatus.TIMEOUT).value(transformedValue.value).build());
	}

}
