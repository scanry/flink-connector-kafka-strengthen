package org.apache.flink.streaming.connectors.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSinkBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

/**
 *
 * @author chinacsci
 * @date 2019年8月28日 上午10:31:33
 * @email 359852326@qq.com
 */
public class StrengthenKafkaTableSourceSinkFactory extends KafkaTableSourceSinkFactoryBase {

	public static final String SPECIFIC_BOUNDED_RULE_EXPR = "specific-bounded-rule-expr";

	@Override
	protected KafkaTableSinkBase createKafkaTableSink(TableSchema schema, String topic, Properties properties,
			Optional<FlinkKafkaPartitioner<Row>> partitioner, SerializationSchema<Row> serializationSchema) {
		return new Kafka011TableSink(schema, topic, properties, partitioner, serializationSchema);
	}


	@Override
	protected KafkaTableSourceBase createKafkaTableSource(TableSchema schema,
			Optional<String> proctimeAttribute, List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Map<String, String> fieldMapping, String topic, Properties properties,
			DeserializationSchema<Row> deserializationSchema, StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets) {
		Object specificBoundedRuleExprOb = properties.remove(SPECIFIC_BOUNDED_RULE_EXPR);
		String specificBoundedRuleExpr = null != specificBoundedRuleExprOb ? specificBoundedRuleExprOb.toString()
				: null;
		if (null != specificBoundedRuleExpr) {
			StrengthenDeserializationSchema proxy = new StrengthenDeserializationSchema(deserializationSchema,
					specificBoundedRuleExpr);
			return new StrengthenKafka011TableSource(schema, proctimeAttribute, rowtimeAttributeDescriptors,
					Optional.of(fieldMapping), topic, properties, proxy, startupMode, specificStartupOffsets, true);
		} else {
			return new StrengthenKafka011TableSource(schema, proctimeAttribute, rowtimeAttributeDescriptors,
					Optional.of(fieldMapping), topic, properties, deserializationSchema, startupMode,
					specificStartupOffsets, false);
		}
	}

	@Override
	protected String kafkaVersion() {
		return "strengthen";
	}

	@Override
	protected boolean supportsKafkaTimestamps() {
		return true;
	}
}
