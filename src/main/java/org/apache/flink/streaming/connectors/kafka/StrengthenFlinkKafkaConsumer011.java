package org.apache.flink.streaming.connectors.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internal.StrengthenKafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;

/**
 *
 * @author chinacsci
 * @date 2019年9月4日 上午10:44:13
 * @email 359852326@qq.com TODO
 */
public class StrengthenFlinkKafkaConsumer011<T> extends FlinkKafkaConsumer010<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7561599057026627498L;

	private boolean isBounded;

	public StrengthenFlinkKafkaConsumer011(String topic, DeserializationSchema<T> valueDeserializer, Properties props,
			boolean isBounded) {
		this(Collections.singletonList(topic), valueDeserializer, props, isBounded);
	}

	public StrengthenFlinkKafkaConsumer011(String topic, KafkaDeserializationSchema<T> deserializer, Properties props,
			boolean isBounded) {
		this(Collections.singletonList(topic), deserializer, props, isBounded);
	}

	public StrengthenFlinkKafkaConsumer011(List<String> topics, DeserializationSchema<T> deserializer, Properties props,
			boolean isBounded) {
		this(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props, isBounded);
	}

	@PublicEvolving
	public StrengthenFlinkKafkaConsumer011(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer,
			Properties props, boolean isBounded) {
		this(subscriptionPattern, new KafkaDeserializationSchemaWrapper<>(valueDeserializer), props, isBounded);
	}

	@PublicEvolving
	public StrengthenFlinkKafkaConsumer011(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer,
			Properties props, boolean isBounded) {
		super(subscriptionPattern, deserializer, props);
		this.isBounded = isBounded;
	}

	public StrengthenFlinkKafkaConsumer011(List<String> topics, KafkaDeserializationSchema<T> deserializer,
			Properties props, boolean isBounded) {
		super(topics, deserializer, props);
		this.isBounded = isBounded;
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		super.run(sourceContext);
	}
	
	@Override
	protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode, MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {
		adjustAutoCommitConfig(properties, offsetCommitMode);
		FlinkConnectorRateLimiter rateLimiter = super.getRateLimiter();
		if (rateLimiter != null) {
			rateLimiter.open(runtimeContext);
		}
		return new StrengthenKafkaFetcher<T>(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic,
				watermarksPunctuated, runtimeContext.getProcessingTimeService(),
				runtimeContext.getExecutionConfig().getAutoWatermarkInterval(), runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getTaskNameWithSubtasks(), deserializer, properties, pollTimeout,
				runtimeContext.getMetricGroup(), consumerMetricGroup, useMetrics, rateLimiter, isBounded) {

		};
	}
}
