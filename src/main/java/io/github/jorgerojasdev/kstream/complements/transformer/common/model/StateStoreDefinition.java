package io.github.jorgerojasdev.kstream.complements.transformer.common.model;

import io.github.jorgerojasdev.kstream.complements.transformer.common.exception.StateStoreDefinitionException;
import io.github.jorgerojasdev.kstream.complements.transformer.common.utils.StateStoreDefinitionUtils;
import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Map;

@Builder
@Getter
public class StateStoreDefinition<V> {

	/**
	 * KeySerde used by StateStore -> default: Serdes.String()
	 */
	private final Serde<?> keySerde;

	/**
	 * ValueSerde used by StateStore -> not default value (must be implemented)
	 */
	private final Serde<V> valueSerde;

	/**
	 * The maxTime that want to save records without forwarding, after this time system will emit in {@see AwaitTransformer#onExpiredRecordAction} -> default: Duration.ofDays(1)
	 */
	private final Duration maxTimeOfSavedRecords;

	/**
	 * The accuracy of analyzing records expiration analysis, very short periods can affect performance -> default: Duration.ofSeconds(1)
	 */
	private final Duration cleaningAccuracy;

	/**
	 * Custom properties of changelog topic created -> default: EMPTY_MAP
	 */
	private final Map<String, String> changelogProperties;

	public static class StateStoreDefinitionBuilder<V> {

		public StateStoreDefinition<V> build() {
			keySerde = keySerde != null ? keySerde : Serdes.String();
			if (valueSerde == null) {
				throw new StateStoreDefinitionException("valueSerde hasn't default value, must be implemented");
			}
			maxTimeOfSavedRecords = maxTimeOfSavedRecords != null ? maxTimeOfSavedRecords : Duration.ofDays(1);
			cleaningAccuracy = cleaningAccuracy != null ? cleaningAccuracy : Duration.ofSeconds(1);

			changelogProperties = changelogProperties == null ? StateStoreDefinitionUtils.defaultStateStoreChangelogPropertiesFromDuration(maxTimeOfSavedRecords) :
					StateStoreDefinitionUtils.overrideStateStoreChangelogPropertiesFromDuration(maxTimeOfSavedRecords, changelogProperties);

			return new StateStoreDefinition<>(keySerde, valueSerde, maxTimeOfSavedRecords, cleaningAccuracy, changelogProperties);
		}
	}
}
