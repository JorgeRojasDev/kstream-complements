package io.github.jorgerojasdev.kstream.complements.transformer.expire.internals;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ExpireInfo<V> {

	private final long onTopicTimestamp;
	private final V value;
}
