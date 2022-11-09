package io.github.jorgerojasdev.kstream.complements.transformer.expire;

import io.github.jorgerojasdev.kstream.complements.transformer.expire.internals.ExpireWrapper;
import lombok.Builder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

import java.util.function.Supplier;

@Builder
public class ExpireTransformerSupplier<K, V, R> implements TransformerSupplier<K, V, KeyValue<K, ExpireWrapper<R>>> {

	private final Supplier<ExpireTransformer<K, V, R>> supplier;

	@Override public Transformer<K, V, KeyValue<K, ExpireWrapper<R>>> get() {
		return supplier.get();
	}
}
