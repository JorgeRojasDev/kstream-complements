package io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.strategy;

import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.enums.LookUpOnEventExistsAction;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.functional.LookUpBiRecordFunction;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.function.BiConsumer;

@Builder
@Slf4j
public class LookUpTransformStrategy<K, V, S, R> implements StateStoreTransformStrategy<K, V, R> {

	private final LookUpBiRecordFunction<K, V, S, KeyValue<K, R>> transformFunction;
	private final String stateStoreName;
	private final LookUpOnEventExistsAction onEventExistsAction;
	private final BiConsumer<K, V> onEventNotExists;

	@SuppressWarnings("unchecked")
	@Override public KeyValue<K, R> transform(Map<String, StateStore> stateStoreMap, ProcessorRecordInfo<K, V> processorRecordInfo) {

		TimestampedKeyValueStore<K, S> timestampedKeyValueStore = (TimestampedKeyValueStore<K, S>) stateStoreMap.get(stateStoreName);

		ValueAndTimestamp<S> valueAndTimestamp = timestampedKeyValueStore.get(processorRecordInfo.getKey());

		if (valueAndTimestamp != null) {
			return executeOnEventExistsAction(timestampedKeyValueStore, processorRecordInfo.getKey(), processorRecordInfo.getValue(), valueAndTimestamp.value());
		}

		if (onEventNotExists != null) {
			onEventNotExists.accept(processorRecordInfo.getKey(), processorRecordInfo.getValue());
		}

		return null;
	}

	private KeyValue<K, R> executeOnEventExistsAction(TimestampedKeyValueStore<K, ?> timestampedKeyValueStore, K key, V value, S storedValue) {
		if (onEventExistsAction == null) {
			timestampedKeyValueStore.delete(key);
			return transformFunction.transform(key, value, storedValue);
		}
		switch (onEventExistsAction) {
		case ONLY_DELETE:
			timestampedKeyValueStore.delete(key);
			return null;
		case ONLY_FORWARD:
			return transformFunction.transform(key, value, storedValue);
		default:
			timestampedKeyValueStore.delete(key);
			return transformFunction.transform(key, value, storedValue);
		}
	}
}
