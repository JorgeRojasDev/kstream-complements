package io.github.jorgerojasdev.kstream.complements.transformer.deduplicate;

import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformerSupplier;
import io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.exception.DeduplicateTransformerException;
import org.apache.commons.lang3.ObjectUtils;

import java.util.function.Supplier;

public class DeduplicateTransformerSupplier<K, V> extends AbstractStatefulTransformerSupplier<K, V, V> {

	private final Supplier<DeduplicateTransformer<K, V>> transformerSupplier;

	public DeduplicateTransformerSupplier(Supplier<DeduplicateTransformer<K, V>> transformerSupplier, Runnable registerStateStoresRunnable, String[] stateStoreNames) {
		super(registerStateStoresRunnable, stateStoreNames);
		if (!ObjectUtils.allNotNull(transformerSupplier, registerStateStoresRunnable, stateStoreNames)) {
			throw new DeduplicateTransformerException("transformerSupplier, registerStateStoresRunnable, stateStoreNames can't be null");
		}
		this.transformerSupplier = transformerSupplier;
	}

	@Override public DeduplicateTransformer<K, V> transformer() {
		return transformerSupplier.get();
	}
}
