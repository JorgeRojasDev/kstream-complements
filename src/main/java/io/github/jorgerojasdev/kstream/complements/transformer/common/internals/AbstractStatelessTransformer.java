package io.github.jorgerojasdev.kstream.complements.transformer.common.internals;

import io.github.jorgerojasdev.kstream.complements.transformer.common.StatelessTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;

public abstract class AbstractStatelessTransformer<K, R, V> implements StatelessTransformer<K, R, V> {

	protected AbstractProcessorContext context;

	@Override public final void init(ProcessorContext context) {
		this.context = (AbstractProcessorContext) context;
	}

	@Override public final void close() {
		//Default close implementation
	}
}
