package io.github.jorgerojasdev.kstream.complements.config;

import io.github.jorgerojasdev.kstream.complements.api.TransformersBuilder;
import io.github.jorgerojasdev.kstream.complements.api.internals.TransformersBuilderImp;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KstreamUtilsAutoConfiguration {

	@Bean
	public TransformersBuilder transformersBuilder(StreamsBuilder streamsBuilder) {
		return new TransformersBuilderImp(streamsBuilder);
	}
}
