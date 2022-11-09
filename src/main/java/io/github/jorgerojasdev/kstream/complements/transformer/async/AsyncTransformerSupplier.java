package io.github.jorgerojasdev.kstream.complements.transformer.async;

import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecord;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformerSupplier;

import java.util.function.Supplier;

public class AsyncTransformerSupplier<K, V, R> extends AbstractStatefulTransformerSupplier<K, V, AsyncRecord<R>> {

	private final Supplier<AsyncTransformer<K, V, R>> supplier;

	public AsyncTransformerSupplier(Runnable registerStateStoresRunnable, String[] stateStoreNames, Supplier<AsyncTransformer<K, V, R>> supplier) {
		super(registerStateStoresRunnable, stateStoreNames);
		this.supplier = supplier;
	}

	@Override public AsyncTransformer<K, V, R> transformer() {
		return supplier.get();
	}
}
