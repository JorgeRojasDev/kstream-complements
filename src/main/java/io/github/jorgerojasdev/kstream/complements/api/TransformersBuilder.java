package io.github.jorgerojasdev.kstream.complements.api;

import io.github.jorgerojasdev.kstream.complements.transformer.async.AsyncTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.async.AsyncTransformerSupplier;
import io.github.jorgerojasdev.kstream.complements.transformer.await.AwaitTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.await.AwaitTransformerSupplier;
import io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.DeduplicateTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.DeduplicateTransformerSupplier;
import io.github.jorgerojasdev.kstream.complements.transformer.expire.ExpireTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.expire.ExpireTransformerSupplier;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.LookUpTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.LookUpTransformerSupplier;

import java.util.function.UnaryOperator;

public interface TransformersBuilder {

	<K, V, R> AwaitTransformerSupplier<K, V, R> await(UnaryOperator<AwaitTransformer.AwaitTransformerBuilder<K, V, R>> awaitTransformerBuilderUnaryOperator);

	<K, V, S, R> LookUpTransformerSupplier<K, V, S, R> lookUp(UnaryOperator<LookUpTransformer.LookUpTransformerBuilder<K, V, S, R>> lookUpTransformerBuilderUnaryOperator);

	<K, V, R> ExpireTransformerSupplier<K, V, R> expire(UnaryOperator<ExpireTransformer.ExpireTransformerBuilder<K, V, R>> clockWardTransformerBuilderUnaryOperator);

	<K, V, R> AsyncTransformerSupplier<K, V, R> async(UnaryOperator<AsyncTransformer.AsyncTransformerBuilder<K, V, R>> asyncTransformerBuilderUnaryOperator);

	<K, V> DeduplicateTransformerSupplier<K, V> deduplicate(UnaryOperator<DeduplicateTransformer.DeduplicateTransformerBuilder<K, V>> deduplicateTransformerBuilderUnaryOperator);
}
