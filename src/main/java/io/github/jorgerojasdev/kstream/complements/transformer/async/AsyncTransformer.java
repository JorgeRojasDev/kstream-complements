package io.github.jorgerojasdev.kstream.complements.transformer.async;

import io.github.jorgerojasdev.kstream.complements.transformer.async.exception.AsyncTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.AsyncRecord;
import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.store.AsyncResponseStateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.async.internals.store.AsyncStateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.async.utils.AsyncExecutorManageUtils;
import io.github.jorgerojasdev.kstream.complements.transformer.async.utils.AsyncResponsesManageUtils;
import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreTransformStrategy;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.common.model.StateStoreDefinition;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
@Builder(toBuilder = true, builderMethodName = "implicit", setterPrefix = "with")
public class AsyncTransformer<K, V, R> extends AbstractStatefulTransformer<K, V, AsyncRecord<R>> {

	private static final Integer DEFAULT_PARALLELISM = 2;
	private final StreamsBuilder streamsBuilder;
	private final String stateStoreName;
	private final StateStoreDefinition<V> stateStoreDefinition;
	private final BiFunction<K, V, R> asyncFunction;
	private final BiFunction<K, V, KeyValue<K, R>> onTimeoutForward;
	private final Function<V, Duration> timeout;
	private final Serde<R> responseSerde;
	private final Integer parallelism;

	@SuppressWarnings("unchecked")
	@Override public AsyncTransformerSupplier<K, V, R> asSupplier() {
		return new AsyncTransformerSupplier<>(() -> registerStateStores(streamsBuilder), this.stateStoreNames(),
				() -> (AsyncTransformer<K, V, R>) this.toBuilder().build());
	}

	@Override public Set<StateStoreExecutor> stateStoreExecutors() {
		return Set.of(stateStoreExecutor(), responseStateStoreExecutor());
	}

	@Override public String[] stateStoreNames() {
		return new String[] { suffixedAsyncStateStoreName(), suffixedAsyncResponsesStateStoreName() };
	}

	@SuppressWarnings("unchecked")
	@Override public StateStoreTransformStrategy<K, V, AsyncRecord<R>> transformStrategy() {
		return ((stateStores, processorRecordInfo) -> {
			TimestampedKeyValueStore<K, V> keyValueStore = (TimestampedKeyValueStore<K, V>) stateStores.get(suffixedAsyncStateStoreName());
			TimestampedKeyValueStore<K, R> responseKeyValueStore = (TimestampedKeyValueStore<K, R>) stateStores.get(suffixedAsyncResponsesStateStoreName());

			CompletableFuture<KeyValue<K, R>> completableFuture = CompletableFuture.supplyAsync(() -> {
				keyValueStore.put(processorRecordInfo.getKey(),
						ValueAndTimestamp.make(processorRecordInfo.getValue(), processorRecordInfo.getCurrentSystemTimestamp()));
				return KeyValue.pair(processorRecordInfo.getKey(), asyncFunction.apply(processorRecordInfo.getKey(), processorRecordInfo.getValue()));
			}, AsyncExecutorManageUtils.getOrCreateExecutor(stateStoreName, parallelism));

			completableFuture.thenAccept(currentKeyValue -> {
				log.trace("Async task finished success");
				ValueAndTimestamp<R> valueAndTimestamp = ValueAndTimestamp.make(currentKeyValue.value, processorRecordInfo.getCurrentSystemTimestamp());
				AsyncResponsesManageUtils.securePutIntoResponseStore(suffixedAsyncResponsesStateStoreName(), responseKeyValueStore, currentKeyValue.key, valueAndTimestamp);
			}).exceptionally(ex -> {
				log.error("AsyncTransfomer error", ex);
				return null;
			});
			return null;
		});
	}

	private AsyncStateStoreExecutor<K, V, R> stateStoreExecutor() {
		return AsyncStateStoreExecutor.<K, V, R>builder()
				.stateStoreName(suffixedAsyncStateStoreName())
				.stateStoreDefinition(stateStoreDefinition)
				.timeoutFunction(timeout)
				.onTimeoutForward(onTimeoutForward)
				.build();
	}

	private AsyncResponseStateStoreExecutor<K, V, R> responseStateStoreExecutor() {
		return AsyncResponseStateStoreExecutor.<K, V, R>builder()
				.responseStateStoreName(suffixedAsyncResponsesStateStoreName())
				.asyncStateStoreName(suffixedAsyncStateStoreName())
				.stateStoreDefinition(StateStoreDefinition.<R>builder()
						.keySerde(stateStoreDefinition.getKeySerde())
						.valueSerde(responseSerde)
						.build())
				.build();
	}

	private String suffixedAsyncStateStoreName() {
		return String.format("%s-async", stateStoreName);
	}

	private String suffixedAsyncResponsesStateStoreName() {
		return String.format("%s-async-responses", stateStoreName);
	}

	public static class AsyncTransformerBuilder<K, V, R> {

		public AsyncTransformer<K, V, R> build() {
			if (!ObjectUtils.allNotNull(stateStoreName, stateStoreDefinition, asyncFunction, onTimeoutForward, timeout, responseSerde)) {
				throw new AsyncTransformerException("stateStoreName, stateStoreDefinition, asyncFunction, onTimeoutForward, timeout, responseSerde can't be null");
			}
			if (parallelism == null) {
				parallelism = DEFAULT_PARALLELISM;
			}

			return new AsyncTransformer<>(streamsBuilder, stateStoreName, stateStoreDefinition, asyncFunction, onTimeoutForward, timeout, responseSerde, parallelism);
		}
	}
}
