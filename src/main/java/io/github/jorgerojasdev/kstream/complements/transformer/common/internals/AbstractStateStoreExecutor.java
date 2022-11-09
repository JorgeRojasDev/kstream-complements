package io.github.jorgerojasdev.kstream.complements.transformer.common.internals;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StateStoreExecutor;

import java.time.Duration;

public abstract class AbstractStateStoreExecutor implements StateStoreExecutor {

	protected final boolean hasExpiredRecord(Long currentTimestamp, Long savedTimestamp, Duration maxSaveDuration) {
		return currentTimestamp - savedTimestamp > maxSaveDuration.toMillis();
	}
}
