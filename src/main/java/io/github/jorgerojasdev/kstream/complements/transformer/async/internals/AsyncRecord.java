package io.github.jorgerojasdev.kstream.complements.transformer.async.internals;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class AsyncRecord<V> {

	private final AsyncRecordStatus status;
	private final V value;
}
