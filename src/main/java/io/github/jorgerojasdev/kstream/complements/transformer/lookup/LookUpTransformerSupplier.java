package io.github.jorgerojasdev.kstream.complements.transformer.lookup;

import io.github.jorgerojasdev.kstream.complements.transformer.await.exception.AwaitTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformerSupplier;
import org.apache.commons.lang3.ObjectUtils;

import java.util.function.Supplier;

public class LookUpTransformerSupplier<K, V, S, R> extends AbstractStatefulTransformerSupplier<K, V, R> {

	private final Supplier<LookUpTransformer<K, V, S, R>> transformerSupplier;

	public LookUpTransformerSupplier(Supplier<LookUpTransformer<K, V, S, R>> transformerSupplier, Runnable registerStateStoresRunnable, String[] stateStoreNames) {
		super(registerStateStoresRunnable, stateStoreNames);
		if (!ObjectUtils.allNotNull(transformerSupplier, registerStateStoresRunnable, stateStoreNames)) {
			throw new AwaitTransformerException("transformerSupplier, registerStateStoresRunnable, stateStoreNames can't be null");
		}
		this.transformerSupplier = transformerSupplier;
	}
	
	@Override public LookUpTransformer<K, V, S, R> transformer() {
		return transformerSupplier.get();
	}
}
