package io.github.jorgerojasdev.kstream.complements.transformer.async.internals.store;

import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecord;
import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecordStatus;
import io.github.jorgerojasdev.kstream.complements.transformer.async.utils.AsyncResponsesManageUtils;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreCleaningStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import java.util.Iterator;

@Builder
@Slf4j
public class AsyncResponseStateStoreExecutor<K, V, R> extends AbstractStateStoreExecutor {

	private final String responseStateStoreName;
	private final String asyncStateStoreName;
	private final StateStoreDefinition<R> stateStoreDefinition;

	@Override public StoreBuilder<?> storeBuilder() {
		return Stores.timestampedKeyValueStoreBuilder(
				Stores.persistentTimestampedKeyValueStore(responseStateStoreName),
				stateStoreDefinition.getKeySerde(),
				stateStoreDefinition.getValueSerde()
		).withLoggingEnabled(stateStoreDefinition.getChangelogProperties());
	}

	@SuppressWarnings("unchecked")
	@Override public StateStoreCleaningStrategy cleaningStrategy() {
		return (stateStoreMap, context) -> {
			TimestampedKeyValueStore<K, R> responsesStateStore = (TimestampedKeyValueStore<K, R>) stateStoreMap.get(responseStateStoreName);
			TimestampedKeyValueStore<K, V> asyncStateStore = (TimestampedKeyValueStore<K, V>) stateStoreMap.get(asyncStateStoreName);

			context.schedule(stateStoreDefinition.getCleaningAccuracy(), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
				Iterator<KeyValue<K, ValueAndTimestamp<R>>> iterator = AsyncResponsesManageUtils.getLostResponsesIterator(responseStateStoreName);
				while (iterator.hasNext()) {
					KeyValue<K, ValueAndTimestamp<R>> next = iterator.next();
					log.debug("Send cached record to stateStore");
					responsesStateStore.put(next.key, next.value);
					AsyncResponsesManageUtils.removeLostResponse(responseStateStoreName, next);
				}
			});

			return context.schedule(stateStoreDefinition.getCleaningAccuracy(), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
				try (KeyValueIterator<K, ValueAndTimestamp<R>> keyValueIterator = responsesStateStore.all()) {

					while (keyValueIterator.hasNext()) {
						KeyValue<K, ValueAndTimestamp<R>> keyValue = keyValueIterator.next();
						if (asyncStateStore.get(keyValue.key) != null) {
							asyncStateStore.delete(keyValue.key);
							context.forward(keyValue.key, AsyncRecord.<R>builder().status(AsyncRecordStatus.OK).value(keyValue.value.value()).build());
						}
						responsesStateStore.delete(keyValue.key);
					}
				}
			});
		};
	}
}
