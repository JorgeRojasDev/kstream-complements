package io.github.jorgerojasdev.kstream.complements.transformer.common.internals;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

@RequiredArgsConstructor
public abstract class AbstractStatefulTransformerSupplier<K, V, R> implements TransformerSupplier<K, V, KeyValue<K, R>> {

	private final Runnable registerStateStoresRunnable;

	private final String[] stateStoreNames;

	public abstract AbstractStatefulTransformer<K, V, R> transformer();

	@Override public final Transformer<K, V, KeyValue<K, R>> get() {
		return transformer();
	}

	public final void registerStateStores() {
		registerStateStoresRunnable.run();
	}

	public final String[] stateStoreNames() {
		return stateStoreNames;
	}
}
