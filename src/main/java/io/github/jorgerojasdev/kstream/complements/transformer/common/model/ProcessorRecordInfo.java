package io.github.jorgerojasdev.kstream.complements.transformer.common.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ProcessorRecordInfo<K, V> {

	private final K key;
	private final V value;
	private final Long currentSystemTimestamp;
	private final Long onInsertTopicTimestamp;
}
