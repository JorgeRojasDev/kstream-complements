package io.github.jorgerojasdev.kstream.complements.transformer.common;

import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Transformer;

public interface StatefulTransformer<K, V, R> extends Transformer<K, V, KeyValue<K, R>>, AutoSupplierTransformer<K, V, R> {

	String[] stateStoreNames();

	StateStoreTransformStrategy<K, V, R> transformStrategy();

	void registerStateStores(StreamsBuilder streamsBuilder);
}
