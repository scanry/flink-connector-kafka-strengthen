package org.apache.flink.streaming.connectors.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

/**
 *
 * @author chinacsci
 * @date 2019年9月3日 下午2:58:27
 * @email 359852326@qq.com 
 */                                                          
public class StrengthenKafka011TableSource extends KafkaTableSourceBase {

	private boolean isBounded;

	public StrengthenKafka011TableSource(TableSchema schema, Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors, Optional<Map<String, String>> fieldMapping,
			String topic, Properties properties, DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode, Map<KafkaTopicPartition, Long> specificStartupOffsets, boolean isBounded) {
		super(schema, proctimeAttribute, rowtimeAttributeDescriptors, fieldMapping, topic, properties,
				deserializationSchema, startupMode, specificStartupOffsets);
		this.isBounded = isBounded;
	}

	public StrengthenKafka011TableSource(TableSchema schema, String topic, Properties properties,
			DeserializationSchema<Row> deserializationSchema, boolean isBounded) {
		super(schema, topic, properties, deserializationSchema);
		this.isBounded = isBounded;
	}

	@Override
	public boolean isBounded() {
		return isBounded;
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties,
			DeserializationSchema<Row> deserializationSchema) {
		return new StrengthenFlinkKafkaConsumer011<>(topic, deserializationSchema, properties, isBounded);
	}
}
