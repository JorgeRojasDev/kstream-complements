package io.github.jorgerojasdev.kstream.complements.transformer.expire;

import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatelessTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.expire.exception.ExpireTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.expire.internals.ExpireInfo;
import io.github.jorgerojasdev.kstream.complements.transformer.expire.internals.ExpireWrapper;
import lombok.Builder;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

@Builder(toBuilder = true, builderMethodName = "implicit", setterPrefix = "with")
public class ExpireTransformer<K, V, R> extends AbstractStatelessTransformer<K, V, ExpireWrapper<R>> {

	private final BiFunction<K, V, KeyValue<K, R>> transformFunction;

	private final Function<ExpireInfo<V>, Long> originTimestampFunction;

	private final Duration durationToBeenExpired;

	@Override public KeyValue<K, ExpireWrapper<R>> transform(K key, V value) {
		ExpireInfo<V> clockWardInfo = ExpireInfo.<V>builder()
				.onTopicTimestamp(this.context.timestamp())
				.value(value)
				.build();
		Long selectedTimestampToCompare = originTimestampFunction.apply(clockWardInfo);
		Long currentTimestamp = this.context.currentSystemTimeMs();
		long lagInMs = currentTimestamp - selectedTimestampToCompare;
		KeyValue<K, R> transformedKeyValue = transformFunction.apply(key, value);

		return KeyValue.pair(transformedKeyValue.key,
				ExpireWrapper.<R>builder()
						.onConsumeTimestamp(this.context.currentSystemTimeMs())
						.onTopicTimestamp(clockWardInfo.getOnTopicTimestamp())
						.lagInMs(lagInMs)
						.selectedTimestampToCompare(selectedTimestampToCompare)
						.isExpired(lagInMs > durationToBeenExpired.toMillis())
						.value(transformedKeyValue.value)
						.build()
		);
	}

	@SuppressWarnings("unchecked")
	@Override public ExpireTransformerSupplier<K, V, R> asSupplier() {
		return ExpireTransformerSupplier.<K, V, R>builder()
				.supplier(() -> (ExpireTransformer<K, V, R>) this.toBuilder().build())
				.build();
	}

	public static class ExpireTransformerBuilder<K, V, R> {

		public ExpireTransformer<K, V, R> build() {
			if (!ObjectUtils.allNotNull(this.transformFunction, this.originTimestampFunction, this.durationToBeenExpired)) {
				throw new ExpireTransformerException("transformFunction, originTimestampFunction and durationToBeenExpired can't be null");
			}
			return new ExpireTransformer<>(transformFunction, originTimestampFunction, durationToBeenExpired);
		}
	}
}
