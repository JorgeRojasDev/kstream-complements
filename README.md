# KStream-Complements Lib

## About

Library intended to unify common and repetitive use cases of kafka-streams


## Installation

~~~

~~~

## Components

- **TransformersBuilder** -> Bean that allows to instantiate the prefabricated transformers:<br><br>

    - **Await** -> Transformer that postpones an event for a certain time, unless it is rescued by another transformer in another subtopology, build example:
      ~~~
      transformerBuilder.await(awaitTransformerBuilder ->
                awaitTransformerBuilder
                        .withStateStoreName("example-functional")
                        .withAwaitDuration(conFiaInDTO -> Duration.ofMinutes(5))
                        .withOverrideAwaiting(false)
                        .withStateStoreDefinition(
                                StateStoreDefinition.<ConFiaInDTO>builder()
                                        .keySerde(Serdes.String())
                                        .valueSerde(serde)
                                        .build())
                        .withOnSaveTimestamp(ProcessorRecordInfo::getCurrentSystemTimestamp)
                        .withTransformFunction(KeyValue::pair));
      ~~~
      |          Method          | Mandatory |  Default   |                                          Type                                          | Description                                              |
      |:------------------------:|:---------:|:--------------------------------------------------------------------------------------:|:-----------------------------------------:|:---------------------------------------------------------|
      |    withStateStoreName    |     Y     |     -      |                                         String                                         | Name of State Store                                      |
      |   withOverrideAwaiting   |     N     |   false    |                                        boolean                                         | Permits override event if this is waiting before         |
      |    withAwaitDuration     |     Y     |     -      |                                        Duration                                        | Expected duration to wake up the event                   |
      |   withOnSaveTimestamp    |     Y     |     -      |          Function<[ProcessorRecordInfo](#processor-record-info)<K, V>, Long>           | Timestamp chosen to control the start time of the wait   |
      |   withOnExpiredRecord    |     N     | No execute |                                    BiConsumer<K, V>                                    | Action to trigger when event is expired (for resilience) |
      |  withTransformFunction   |     Y     |     -      |                            BiFunction<K, V, KeyValue<K, R>>                            | Function to transform input event to output event class  |
      | withStateStoreDefinition |     Y     |     -      |                 [StateStoreDefinition](#state-store-definition)<<V>V>                  | State Store info and definition to register              |
    <br>

    - **LookUp** ->
      Transformer that looks for an element with the same key in a previously created state store, can delete and forward the record (default), just delete or forward it

      ~~~
      transformerBuilder.lookUp(lookUpTransformerBuilder ->
                lookUpTransformerBuilder
                        .withStateStoreName("example-functional")
                        .withLookUpStateStoreSuffix(LookUpStateStoreSuffix.AWAIT)
                        .withOnEventExistsAction(LookUpOnEventExistsAction.DELETE_AND_FORWARD)
                        .withTransformFunction(KeyValue::pair)
      ~~~
      |            Method             | Mandatory |      Default       |                      Type                       | Description                                                                                                                                                                                                                                                                                  |
      |:-----------------------------:|:---------:|:-----------------------------------------------:|:--------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
      |      withStateStoreName       |     Y     |         -          |                     String                      | Name of State Store                                                                                                                                                                                                                                                                          |
      |  withLookUpStateStoreSuffix   |     N     |        NONE        |             LookUpStateStoreSuffix              | Agree suffix to state store name: (NONE -> stateStoreName without suffix, AWAIT -> with await standard suffix, to wake up awaiting event)                                                                                                                                                    |
      |     withTransformFunction     |     Y     |         -          | LookUpBiRecordFunction<K, V, S, KeyValue<K, R>> | Function to transform input event to output event class                                                                                                                                                                                                                                      |
      | withLookUpOnEventExistsAction |     N     | DELETE_AND_FORWARD |            LookUpOnEventExistsAction            | Strategy to apply when event is previously stored: (DELETE_AND_FORWARD -> Deletes previous stored event (disable awaiting event) and forward, ONLY_FORWARD -> It doesn't do nothing at awaiting event, ONLY_DELETE -> It doesn't forward current event, only disable previous awaiting event |
   <br>

    - **Expire** -> Transformer dedicated to control the time of the record from its creation or insertion in the topic and provide information on forward to have control of
      expired events

         ~~~
          transformersBuilder.expire(expireTransformerBuilder ->
                expireTransformerBuilder
                        .withTransformFunction(KeyValue::pair)
                        .withOriginTimestampFunction(expireInfo ->
                                expireInfo.getValue().getTimestamp()
                        )
                        .withDurationToBeenExpired(Duration.ofHours(1));
         ~~~  
      |              Method              | Mandatory | Default |               Type               | Description                                             |
      |:--------------------------------:|:---------:|:-------:|:--------------------------------:|:--------------------------------------------------------|
      |    withDurationToBeenExpired     |     Y     |    -    |             Duration             | Minimum duration to consider a registration expired     |
      |   withOriginTimestampFunction    |     Y     |    -    |  Function<ExpireInfo<V>, Long>   | Function to choose the timestamp used to compare        |
      |      withTransformFunction       |     Y     |    -    | BiFunction<K, V, KeyValue<K, R>> | Function to transform input event to output event class |

  - **Async** -> Transformer dedicated to execute asynchronous code on parallel

       ~~~
		  transformersBuilder.async(builder ->
				builder
						.withStateStoreName("example-functional")
						.withStateStoreDefinition(
								StateStoreDefinition.<ConFiaInDTO>builder()
										.keySerde(Serdes.String())
										.valueSerde(serde)
										.build())
						.withTimeout(conFiaInDTO -> Duration.ofSeconds(3))
						.withOnTimeoutForward(KeyValue::pair)
						.withAsyncFunction((key, value) -> {
							//Asynchronous Code
							return KeyValue.pair(key, value);
						})
		  );
       ~~~  
      |          Method          | Mandatory | Default |               Type               | Description                        |
      |:------------------------:|:---------:|:-------:|:--------------------------------:|:-----------------------------------|
      |    withStateStoreName    |     Y     |    -    |              String              | State Store name                   |
      | withStateStoreDefinition |     Y     |    -    |       StateStoreDefinition       | Definition of StateStore           |
      |       withTimeout        |     Y     |    -    |      Function<V, Duration>       | Returns duration to timeout        |
      |    withTimeoutForward    |     Y     |    -    | BiFunction<K, V, KeyValue<K, R>> | Function to use forward on timeout |
      |    withAsyncFunction     |     Y     |    -    | BiFunction<K, V, KeyValue<K, R>> | Async Function to execute          |

## Common:
   - <strong id="processor-record-info">ProcessorRecordInfo<K, V></strong> -> Information of the processor record:

     |         Field          | Type | Description                                                  |
     |:----------------------:|:----:|:-------------------------------------------------------------|
     |          key           |  K   | Current key of event                                         |
     |         value          |  V   | Current value of event                                       |
     | currentSystemTimestamp | Long | Current timestamp of system, like System.currentTimeMillis() |
     | onInsertTopicTimestamp | Long | On topic inserted timestamp of event                         |
<br>

   - <strong id="state-store-definition">StateStoreDefinition<V<V>></strong> -> Information of the state store:

     |         Field         |        Type         | Description                                             |
     |:---------------------:|:-------------------:|:--------------------------------------------------------|
     |       keySerde        |      Serde<?>       | Key Serde                                               |
     |      valueSerde       |     Serde<<V>V>     | Value Serde                                             |
     | maxTimeOfSavedRecords |      Duration       | After this time the event will be deleted on StateStore |
     |   cleaningAccuracy    |      Duration       | Duration between cleaning-cycle, that defines accuracy  |
     |  changelogProperties  | Map<String, String> | Changelog properties to override default                |

## Dependencies:

- Java 11+
- Maven 3.6+
- lombok
- mapstruct
- spring-kafka
- kafka-streams
- kafka-streams-avro-serde
- commons-text
