package io.github.jorgerojasdev.kstream.complements.transformer.async.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AsyncResponsesManageUtils {

	private static final Map<String, List<KeyValue<?, ValueAndTimestamp<?>>>> lostResponsesByStateStoreClosed = new HashMap<>();

	public static <K, R> void securePutIntoResponseStore(String keyValueStoreName, TimestampedKeyValueStore<K, R> keyValueStore, K key, ValueAndTimestamp<R> valueAndTimestamp) {
		try {
			keyValueStore.put(key, valueAndTimestamp);
		} catch (Exception e) {
			log.debug("Catching save on stateStore to local cache", e);
			if (!lostResponsesByStateStoreClosed.containsKey(keyValueStoreName)) {
				lostResponsesByStateStoreClosed.put(keyValueStoreName, new ArrayList<>());
			}
			lostResponsesByStateStoreClosed.get(keyValueStoreName).add(KeyValue.pair(key, valueAndTimestamp));
		}
	}

	@SuppressWarnings("unchecked")
	public static <K, R> Iterator<KeyValue<K, ValueAndTimestamp<R>>> getLostResponsesIterator(String keyValueStoreName) {
		List<KeyValue<?, ValueAndTimestamp<?>>> list = lostResponsesByStateStoreClosed.get(keyValueStoreName);
		if (CollectionUtils.isEmpty(list)) {
			list = new ArrayList<>();
		}
		List<KeyValue<K, ValueAndTimestamp<R>>> castedList = list.stream().map(keyValue ->
				KeyValue.pair((K) keyValue.key, ValueAndTimestamp.make((R) keyValue.value.value(), keyValue.value.timestamp()))
		).collect(Collectors.toList());

		return castedList.iterator();
	}

	public static <K, R> void removeLostResponse(String keyValueStoreName, KeyValue<K, ValueAndTimestamp<R>> lostResponse) {
		if (!lostResponsesByStateStoreClosed.containsKey(keyValueStoreName)) {
			return;
		}
		lostResponsesByStateStoreClosed.get(keyValueStoreName).remove(lostResponse);
	}

}
