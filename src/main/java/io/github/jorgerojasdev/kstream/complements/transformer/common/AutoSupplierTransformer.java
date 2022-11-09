package io.github.jorgerojasdev.kstream.complements.transformer.common;

import org.apache.kafka.streams.kstream.TransformerSupplier;

public interface AutoSupplierTransformer<K, V, R> {

	<S extends TransformerSupplier<K, V, R>> S asSupplier();
}
