package io.github.jorgerojasdev.kstream.complements.transformer.common.exception;

import org.apache.kafka.streams.errors.TopologyException;

public class StateStoreDefinitionException extends TopologyException {

	public StateStoreDefinitionException(String message) {
		super(message);
	}
}
