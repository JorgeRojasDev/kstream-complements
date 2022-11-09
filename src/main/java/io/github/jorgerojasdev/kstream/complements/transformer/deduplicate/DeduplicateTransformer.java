package io.github.jorgerojasdev.kstream.complements.transformer.deduplicate;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.ProcessorRecordInfo;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.exception.DeduplicateTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.deduplicate.internals.store.DeduplicateStateStoreExecutor;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

@Slf4j
@Builder(setterPrefix = "with", builderMethodName = "implicit", toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DeduplicateTransformer<K, V> extends AbstractStatefulTransformer<K, V, V> {

	private static final String DEDUPLICATE_STATE_STORE_SUFFIX = "deduplicate";

	private final StreamsBuilder streamsBuilder;
	private final String stateStoreName;
	private final Function<V, Duration> deduplicateDuration;
	private final Function<ProcessorRecordInfo<K, V>, Long> onSaveTimestamp;
	private final StateStoreDefinition<V> stateStoreDefinition;

	@Override public final Set<StateStoreExecutor> stateStoreExecutors() {
		Set<StateStoreExecutor> stateStoreExecutors = new HashSet<>();
		stateStoreExecutors.add(deduplicateStateStoreExecutor());
		return stateStoreExecutors;
	}

	@SuppressWarnings("unchecked")
	@Override public final StateStoreTransformStrategy<K, V, V> transformStrategy() {
		return ((stateStoreMap, processorRecordInfo) -> {
			TimestampedKeyValueStore<K, V> timestampedKeyValueStore = (TimestampedKeyValueStore<K, V>) stateStoreMap.get(suffixedStateStoreName());
			ValueAndTimestamp<V> valueAndTimestamp = timestampedKeyValueStore.get(processorRecordInfo.getKey());

			if (valueAndTimestamp == null) {
				timestampedKeyValueStore.put(processorRecordInfo.getKey(), ValueAndTimestamp.make(processorRecordInfo.getValue(), onSaveTimestamp.apply(processorRecordInfo)));
				return KeyValue.pair(processorRecordInfo.getKey(), processorRecordInfo.getValue());
			}

			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Override public DeduplicateTransformerSupplier<K, V> asSupplier() {
		return new DeduplicateTransformerSupplier<>(() -> (DeduplicateTransformer<K, V>) this.toBuilder().build(), () -> this.registerStateStores(streamsBuilder),
				stateStoreNames());
	}

	private String suffixedStateStoreName() {
		return String.format("%s-%s", stateStoreName, DEDUPLICATE_STATE_STORE_SUFFIX);
	}

	public String[] stateStoreNames() {
		return new String[] { suffixedStateStoreName() };
	}

	private DeduplicateStateStoreExecutor<K, V> deduplicateStateStoreExecutor() {
		DeduplicateStateStoreExecutor.DeduplicateStateStoreExecutorBuilder<K, V> deduplicateStateStoreExecutorBuilder = DeduplicateStateStoreExecutor.<K, V>builder()
				.stateStoreName(suffixedStateStoreName())
				.deduplicateDuration(deduplicateDuration)
				.stateStoreDefinition(stateStoreDefinition);
		return deduplicateStateStoreExecutorBuilder.build();
	}

	public static <K, V, R> DeduplicateTransformer.DeduplicateTransformerBuilder<K, V> builder(StreamsBuilder streamsBuilder) {
		DeduplicateTransformer.DeduplicateTransformerBuilder<K, V> awaitTransformerBuilder = DeduplicateTransformer.implicit();
		awaitTransformerBuilder.streamsBuilder = streamsBuilder;
		return awaitTransformerBuilder;
	}

	public static class DeduplicateTransformerBuilder<K, V> {

		public DeduplicateTransformer<K, V> build() {
			validateAttrs();
			return new DeduplicateTransformer<>(streamsBuilder, stateStoreName, deduplicateDuration, onSaveTimestamp, stateStoreDefinition);
		}

		private void validateAttrs() {
			Object[] mandatoryObjects = new Object[] { streamsBuilder, stateStoreName, deduplicateDuration, onSaveTimestamp, stateStoreDefinition };
			if (!ObjectUtils.allNotNull(mandatoryObjects)) {
				throw new DeduplicateTransformerException(
						"streamsBuilder, stateStoreName, deduplicateDuration, onSaveTimestamp, stateStoreDefinition can't be null");
			}
		}
	}
}
