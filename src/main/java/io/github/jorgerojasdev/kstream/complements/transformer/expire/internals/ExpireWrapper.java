package io.github.jorgerojasdev.kstream.complements.transformer.expire.internals;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class ExpireWrapper<V> {

	private final boolean isExpired;
	private final long lagInMs;
	private final long onTopicTimestamp;
	private final long onConsumeTimestamp;
	private final long selectedTimestampToCompare;
	private final V value;
}
