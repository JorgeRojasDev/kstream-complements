package io.github.jorgerojasdev.kstream.complements.transformer.lookup;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;
import io.github.jorgerojasdev.kstream.complements.transformer.common.internals.AbstractStatefulTransformer;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.exception.LookUpTransformerException;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.enums.LookUpOnEventExistsAction;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.enums.LookUpStateStoreSuffix;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.functional.LookUpBiRecordFunction;
import io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.strategy.LookUpTransformStrategy;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

@Slf4j
@Builder(setterPrefix = "with", builderMethodName = "implicit", toBuilder = true)
public class LookUpTransformer<K, V, S, R> extends AbstractStatefulTransformer<K, V, R> {

	private final StreamsBuilder streamsBuilder;
	private final String stateStoreName;
	private final LookUpStateStoreSuffix lookUpStateStoreSuffix;
	private final LookUpBiRecordFunction<K, V, S, KeyValue<K, R>> transformFunction;
	private final LookUpOnEventExistsAction onEventExistsAction;
	private final BiConsumer<K, V> onEventNotExists;

	@Override public final Set<StateStoreExecutor> stateStoreExecutors() {
		return new HashSet<>();
	}

	@SuppressWarnings("unchecked")
	@Override public final LookUpTransformStrategy<K, V, S, R> transformStrategy() {
		return LookUpTransformStrategy.<K, V, S, R>builder()
				.stateStoreName(suffixedStateStoreName())
				.transformFunction(transformFunction)
				.onEventExistsAction(onEventExistsAction)
				.onEventNotExists(onEventNotExists)
				.build();
	}

	@SuppressWarnings("unchecked")
	@Override public LookUpTransformerSupplier<K, V, S, R> asSupplier() {
		return new LookUpTransformerSupplier<>(() -> (LookUpTransformer<K, V, S, R>) this.toBuilder().build(),
				() -> log.debug("LookUpTransformer can't register any stores, only consume that"),
				stateStoreNames());
	}

	private String suffixedStateStoreName() {
		if (lookUpStateStoreSuffix.getSuffix() == null) {
			return stateStoreName;
		}
		return String.format("%s-%s", stateStoreName, lookUpStateStoreSuffix.getSuffix());
	}

	public String[] stateStoreNames() {
		return new String[] { suffixedStateStoreName() };
	}

	public static <K, V, S, R> LookUpTransformerBuilder<K, V, S, R> builder(StreamsBuilder streamsBuilder) {
		LookUpTransformerBuilder<K, V, S, R> lookUpTransformerBuilder = LookUpTransformer.implicit();
		lookUpTransformerBuilder.streamsBuilder = streamsBuilder;
		return lookUpTransformerBuilder;
	}

	public static class LookUpTransformerBuilder<K, V, S, R> {

		public LookUpTransformer<K, V, S, R> build() {
			validateAttrs();
			return new LookUpTransformer<>(streamsBuilder, stateStoreName, lookUpStateStoreSuffix, transformFunction, onEventExistsAction, onEventNotExists);
		}

		private void validateAttrs() {
			Object[] mandatoryObjects = new Object[] { streamsBuilder, stateStoreName, transformFunction };
			if (!ObjectUtils.allNotNull(mandatoryObjects)) {
				throw new LookUpTransformerException(
						"streamsBuilder, stateStoreName, transformFunction can't be null");
			}
			if (lookUpStateStoreSuffix == null) {
				lookUpStateStoreSuffix = LookUpStateStoreSuffix.NONE;
			}
		}
	}
}
