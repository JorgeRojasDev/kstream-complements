package io.github.jorgerojasdev.kstream.complements.transformer.await.internals.strategy;

import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.function.Function;

@Builder
@Slf4j
public class AwaitTransformStrategy<K, V, R> implements StateStoreTransformStrategy<K, V, R> {

	private final boolean overrideAwaiting;

	private final String stateStoreName;

	private final Function<ProcessorRecordInfo<K, V>, Long> onSaveTimestamp;

	@SuppressWarnings("unchecked")
	@Override public KeyValue<K, R> transform(Map<String, StateStore> stateStoreMap, ProcessorRecordInfo<K, V> processorRecordInfo) {
		TimestampedKeyValueStore<K, V> timestampedKeyValueStore = (TimestampedKeyValueStore<K, V>) stateStoreMap.get(stateStoreName);

		if (overrideAwaiting) {
			timestampedKeyValueStore.put(processorRecordInfo.getKey(), ValueAndTimestamp.make(processorRecordInfo.getValue(), onSaveTimestamp.apply(processorRecordInfo)));
			return null;
		}

		timestampedKeyValueStore.putIfAbsent(processorRecordInfo.getKey(), ValueAndTimestamp.make(processorRecordInfo.getValue(), onSaveTimestamp.apply(processorRecordInfo)));
		return null;
	}
}
