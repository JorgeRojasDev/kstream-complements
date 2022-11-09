package io.github.jorgerojasdev.kstream.complements.transformer.common.functional;

import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;

import java.util.Map;

@FunctionalInterface
public interface StateStoreTransformStrategy<K, V, R> {

	KeyValue<K, R> transform(Map<String, StateStore> stateStores, ProcessorRecordInfo<K, V> processorRecordInfo);
}
