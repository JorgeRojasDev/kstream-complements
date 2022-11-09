package io.github.jorgerojasdev.kstream.complements.transformer.await;

import io.github.jorgerojasdev.kstream.complements.transformer.await.exception.AwaitTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformerSupplier;
import org.apache.commons.lang3.ObjectUtils;

import java.util.function.Supplier;

public class AwaitTransformerSupplier<K, V, R> extends AbstractStatefulTransformerSupplier<K, V, R> {

	private final Supplier<AwaitTransformer<K, V, R>> transformerSupplier;

	public AwaitTransformerSupplier(Supplier<AwaitTransformer<K, V, R>> transformerSupplier, Runnable registerStateStoresRunnable, String[] stateStoreNames) {
		super(registerStateStoresRunnable, stateStoreNames);
		if (!ObjectUtils.allNotNull(transformerSupplier, registerStateStoresRunnable, stateStoreNames)) {
			throw new AwaitTransformerException("transformerSupplier, registerStateStoresRunnable, stateStoreNames can't be null");
		}
		this.transformerSupplier = transformerSupplier;
	}
	
	@Override public AwaitTransformer<K, V, R> transformer() {
		return transformerSupplier.get();
	}
}
