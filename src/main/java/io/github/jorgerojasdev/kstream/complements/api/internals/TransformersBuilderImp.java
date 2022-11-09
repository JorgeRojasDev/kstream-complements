package io.github.jorgerojasdev.kstream.complements.api.internals;

import io.github.jorgerojasdev.kstream.complements.api.TransformersBuilder;
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
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.function.UnaryOperator;

@RequiredArgsConstructor
public class TransformersBuilderImp implements TransformersBuilder {

	private final StreamsBuilder streamsBuilder;

	@Override public <K, V, R> AwaitTransformerSupplier<K, V, R> await(UnaryOperator<AwaitTransformer.AwaitTransformerBuilder<K, V, R>> unaryOperator) {
		return unaryOperator.apply(AwaitTransformer.builder(streamsBuilder)).build().asSupplier();
	}

	@Override public <K, V, S, R> LookUpTransformerSupplier<K, V, S, R> lookUp(UnaryOperator<LookUpTransformer.LookUpTransformerBuilder<K, V, S, R>> unaryOperator) {
		return unaryOperator.apply(LookUpTransformer.builder(streamsBuilder)).build().asSupplier();
	}

	@Override public <K, V, R> ExpireTransformerSupplier<K, V, R> expire(UnaryOperator<ExpireTransformer.ExpireTransformerBuilder<K, V, R>> unaryOperator) {
		return unaryOperator.apply(ExpireTransformer.implicit()).build().asSupplier();
	}

	@Override public <K, V, R> AsyncTransformerSupplier<K, V, R> async(UnaryOperator<AsyncTransformer.AsyncTransformerBuilder<K, V, R>> unaryOperator) {
		return unaryOperator.apply(AsyncTransformer.<K, V, R>implicit().withStreamsBuilder(streamsBuilder)).build().asSupplier();
	}

	@Override public <K, V> DeduplicateTransformerSupplier<K, V> deduplicate(UnaryOperator<DeduplicateTransformer.DeduplicateTransformerBuilder<K, V>> unaryOperator) {
		return unaryOperator.apply(DeduplicateTransformer.<K, V>implicit().withStreamsBuilder(streamsBuilder)).build().asSupplier();
	}
}
