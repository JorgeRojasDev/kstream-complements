package io.github.jorgerojasdev.kstream.complements.transformer.common.functional;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;

import java.util.Map;

@FunctionalInterface
public interface StateStoreCleaningStrategy {

	Cancellable scheduleCleaningStrategy(Map<String, StateStore> stateStoreMap, AbstractProcessorContext context);
}
