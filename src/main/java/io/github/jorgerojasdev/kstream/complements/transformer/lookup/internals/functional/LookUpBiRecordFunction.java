package io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.functional;

@FunctionalInterface
public interface LookUpBiRecordFunction<K, V, S, R> {

	R transform(K key, V value, S storedValue);
}
