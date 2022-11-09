package io.github.jorgerojasdev.kstream.complements.transformer.common.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StateStoreDefinitionUtils {

	public static Map<String, String> defaultStateStoreChangelogPropertiesFromDuration(Duration maxTimeOfSavedRecords) {
		return Map.ofEntries(
				Map.entry("cleanup.policy", "delete"),
				Map.entry("segment.bytes", "536870912"),
				Map.entry("min.cleanable.dirty.ratio", "0.1"),
				Map.entry("retention.ms", String.valueOf(maxTimeOfSavedRecords.toMillis()))
		);
	}

	public static Map<String, String> overrideStateStoreChangelogPropertiesFromDuration(Duration maxTimeOfSavedRecords, Map<String, String> overrideParams) {
		Map<String, String> mergeMap = new HashMap<>(defaultStateStoreChangelogPropertiesFromDuration(maxTimeOfSavedRecords));
		overrideParams.forEach((key, value) -> mergeMap.merge(key, value, (m1, m2) -> m2));
		return mergeMap;
	}
}
