package io.github.jorgerojasdev.kstream.complements.transformer.common;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

public interface StatelessTransformer<K, V, R> extends Transformer<K, V, KeyValue<K, R>>, AutoSupplierTransformer<K, V, R> {

}
