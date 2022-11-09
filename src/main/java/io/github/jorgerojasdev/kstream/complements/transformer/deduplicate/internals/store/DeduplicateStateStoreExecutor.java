package io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.internals.store;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreCleaningStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import lombok.Builder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.function.Function;

@Builder
public class DeduplicateStateStoreExecutor<K, V> implements StateStoreExecutor {

	private final String stateStoreName;
	private final StateStoreDefinition<V> stateStoreDefinition;
	private final Function<V, Duration> deduplicateDuration;

	@Override public StoreBuilder<?> storeBuilder() {
		return Stores.timestampedKeyValueStoreBuilder(
				Stores.persistentTimestampedKeyValueStore(stateStoreName),
				stateStoreDefinition.getKeySerde(),
				stateStoreDefinition.getValueSerde()
		).withLoggingEnabled(stateStoreDefinition.getChangelogProperties());
	}

	@SuppressWarnings("unchecked")
	@Override public final StateStoreCleaningStrategy cleaningStrategy() {
		return ((stateStoreMap, context) -> {
			TimestampedKeyValueStore<K, V> stateStore = (TimestampedKeyValueStore<K, V>) stateStoreMap.get(stateStoreName);

			return context.schedule(stateStoreDefinition.getCleaningAccuracy(), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
				try (KeyValueIterator<K, ValueAndTimestamp<V>> keyValueIterator = stateStore.all()) {
					while (keyValueIterator.hasNext()) {
						KeyValue<K, ValueAndTimestamp<V>> keyValue = keyValueIterator.next();
						long currentTimestamp = context.currentSystemTimeMs();

						if (hasExpiredRecord(currentTimestamp, keyValue.value.timestamp(), deduplicateDuration.apply(keyValue.value.value()))) {
							onExpiredMaxTimeRecord(stateStore, keyValue);
						}
					}
				}
			});
		});
	}

	private void onExpiredMaxTimeRecord(TimestampedKeyValueStore<K, V> stateStore, KeyValue<K, ValueAndTimestamp<V>> keyValue) {
		stateStore.delete(keyValue.key);
	}

	private boolean hasExpiredRecord(Long currentTimestamp, Long savedTimestamp, Duration maxSaveDuration) {
		return currentTimestamp - savedTimestamp > maxSaveDuration.toMillis();
	}
}
