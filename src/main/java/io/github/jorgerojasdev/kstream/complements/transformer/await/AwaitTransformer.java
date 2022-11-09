package io.github.jorgerojasdev.kstream.complements.transformer.await;

import io.github.jorgerojasdev.kstream.complements.transformer.await.exception.AwaitTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.await.internals.store.AwaitMainStateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.await.internals.strategy.AwaitTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
@Builder(setterPrefix = "with", builderMethodName = "implicit", toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AwaitTransformer<K, V, R> extends AbstractStatefulTransformer<K, V, R> {

	private static final String AWAIT_STATE_STORE_SUFFIX = "await";

	private final StreamsBuilder streamsBuilder;
	private final String stateStoreName;
	private final boolean overrideAwaiting;
	private final Function<V, Duration> awaitDuration;
	private final Function<ProcessorRecordInfo<K, V>, Long> onSaveTimestamp;
	private final BiConsumer<K, V> onExpiredRecord;
	private final BiFunction<K, V, KeyValue<K, R>> transformFunction;
	private final StateStoreDefinition<V> stateStoreDefinition;

	@Override public final Set<StateStoreExecutor> stateStoreExecutors() {
		Set<StateStoreExecutor> stateStoreExecutors = new HashSet<>();
		stateStoreExecutors.add(awaitStateStoreExecutor());
		return stateStoreExecutors;
	}

	@Override public final StateStoreTransformStrategy<K, V, R> transformStrategy() {
		return AwaitTransformStrategy.<K, V, R>builder()
				.stateStoreName(suffixedStateStoreName())
				.overrideAwaiting(overrideAwaiting)
				.onSaveTimestamp(onSaveTimestamp)
				.build();
	}

	@SuppressWarnings("unchecked")
	@Override public AwaitTransformerSupplier<K, V, R> asSupplier() {
		return new AwaitTransformerSupplier<>(() -> (AwaitTransformer<K, V, R>) this.toBuilder().build(), () -> this.registerStateStores(streamsBuilder), stateStoreNames());
	}

	private String suffixedStateStoreName() {
		return String.format("%s-%s", stateStoreName, AWAIT_STATE_STORE_SUFFIX);
	}

	public String[] stateStoreNames() {
		return new String[] { suffixedStateStoreName() };
	}

	private AwaitMainStateStoreExecutor<K, V, R> awaitStateStoreExecutor() {
		AwaitMainStateStoreExecutor.AwaitMainStateStoreExecutorBuilder<K, V, R> awaitMainStateStoreExecutorBuilder = AwaitMainStateStoreExecutor.<K, V, R>builder()
				.stateStoreName(suffixedStateStoreName())
				.awaitDuration(awaitDuration)
				.stateStoreDefinition(stateStoreDefinition)
				.onExpiredRecord(onExpiredRecord)
				.transformFunction(transformFunction);
		return awaitMainStateStoreExecutorBuilder.build();
	}

	public static <K, V, R> AwaitTransformerBuilder<K, V, R> builder(StreamsBuilder streamsBuilder) {
		AwaitTransformerBuilder<K, V, R> awaitTransformerBuilder = AwaitTransformer.implicit();
		awaitTransformerBuilder.streamsBuilder = streamsBuilder;
		return awaitTransformerBuilder;
	}

	public static class AwaitTransformerBuilder<K, V, R> {

		public AwaitTransformer<K, V, R> build() {
			validateAttrs();
			return new AwaitTransformer<>(streamsBuilder, stateStoreName, overrideAwaiting, awaitDuration, onSaveTimestamp, onExpiredRecord, transformFunction,
					stateStoreDefinition);
		}

		private void validateAttrs() {
			Object[] mandatoryObjects = new Object[] { streamsBuilder, stateStoreName, awaitDuration, onSaveTimestamp, transformFunction,
					stateStoreDefinition };
			if (!ObjectUtils.allNotNull(mandatoryObjects)) {
				throw new AwaitTransformerException(
						"streamsBuilder, stateStoreName, awaitDuration, onSaveTimestamp, transformFunction, stateStoreDefinition can't be null");
			}
		}
	}
}
