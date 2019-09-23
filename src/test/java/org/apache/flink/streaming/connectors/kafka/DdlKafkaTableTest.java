package org.apache.flink.streaming.connectors.kafka;

import java.util.UUID;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 *
 * @author chinacsci
 * @date   2019年8月28日 上午10:43:37
 * @email  359852326@qq.com
 * TODO
 */
public class DdlKafkaTableTest{

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment execEnv=StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings environmentSettings=EnvironmentSettings.newInstance().useBlinkPlanner().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv,environmentSettings);
		//rest.bind-port
		String tableName = "pfcompy_basicinfo";
//		String createTableSql="create table "+tableName+"("
//				+ "					a int,"
//				+ "					b int,"
//				+ "					c int,"
//				+ "					d varchar"
//				+ "            ) with("
//				+ "            'connector.bootstrap-servers' = '10.100.44.46:6667',"
//				+ "            'connector.type' = '"+KafkaConnectorDescriptorValidator.CONNECTOR_TYPE_VALUE+"',"
//				+ "            'connector.topic' = '"+tableName+"',"
//				+ "            'format.type' = 'json'"
//				+ "			   )";

		String createTableSql="create table "+tableName+"("
				+ "					id           BIGINT,"
				+ "					row_id       BIGINT,"
				+ "					batch_id     BIGINT,"
				+ "					company_id   BIGINT,"
				+ "					company_nm   VARCHAR,"
				+ "					deleted      INT"
				+ "            ) with ("
				+ "            'connector.type' = 'kafka',"
				+ "            'update-mode' = 'append',"
				+ "            'connector.version' = 'strengthen',"
				+ "            'connector.property-version' = '1',"
				
				+ "            'connector.properties.0.key' = 'bootstrap.servers',"
				+ "            'connector.properties.0.value' = '10.100.44.46:6667',"				
				+ "            'connector.properties.1.key' = 'group.id',"
				+ "            'connector.properties.1.value' = '"+UUID.randomUUID().toString() + System.currentTimeMillis()+"',"				
				+ "            'connector.properties.2.key' = 'specific-bounded-rule-expr',"
				+ "            'connector.properties.2.value' = 'gt(key(batch_id),num(1168442235428012032))',"
				
				+ "            'connector.topic' = '"+tableName+"',"
				+ "            'connector.startup-mode' = 'specific-offsets',"				
				+ "            'connector.specific-offsets.0.partition' = '0',"
				+ "            'connector.specific-offsets.0.offset' = '1',"			
				+ "            'connector.specific-offsets.1.partition' = '1',"
				+ "            'connector.specific-offsets.1.offset' = '1',"				
				+ "            'connector.specific-offsets.2.partition' = '2',"
				+ "            'connector.specific-offsets.2.offset' = '1',"
				+ "            'format.type' = 'json',"
				+ "            'format.derive-schema' = 'true'"
				+ "			   )";
		
		tEnv.sqlUpdate(createTableSql);
		String bizQuerySql = "select * from "+tableName+" where company_id=125588";

		Table bizQueryTable = tEnv.sqlQuery(bizQuerySql);
		tEnv.toRetractStream(bizQueryTable,bizQueryTable.getSchema().toRowType()).print();
		execEnv.execute("Flink Streaming Java API Skeleton");
	}
}
