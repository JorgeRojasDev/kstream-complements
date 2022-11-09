package io.github.jorgerojasdev.kstream.complements.transformer.lookup.internals.enums;

import lombok.Getter;

@Getter
public enum LookUpStateStoreSuffix {
	NONE(null), AWAIT("await");

	private final String suffix;

	LookUpStateStoreSuffix(String suffix) {
		this.suffix = suffix;
	}
}
