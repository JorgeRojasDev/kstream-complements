package io.github.jorgerojasdev.kstream.complements.transformer.common;

import io.github.jorgerojasdev.kstream.complements.transformer.common.functional.StateStoreCleaningStrategy;
import org.apache.kafka.streams.state.StoreBuilder;

public interface StateStoreExecutor {

	StoreBuilder<?> storeBuilder();

	StateStoreCleaningStrategy cleaningStrategy();
}
