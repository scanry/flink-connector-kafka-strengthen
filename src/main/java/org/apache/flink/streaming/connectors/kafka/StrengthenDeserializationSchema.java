package org.apache.flink.streaming.connectors.kafka;

import java.io.IOException;
import java.util.Collections;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.util.GenericTokenParserUtils;
import org.apache.flink.streaming.connectors.kafka.util.TypeInformationUtils;
import org.apache.flink.types.Row;

import com.chinacscs.basicplatform.commons.expr.FuncFactory.Func;
import com.chinacscs.basicplatform.commons.expr.FuncFactorys;

/**
 *
 * @author chinacsci
 * @date 2019年9月3日 下午2:59:57
 * @email 359852326@qq.com TODO
 */
public class StrengthenDeserializationSchema implements DeserializationSchema<Row> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1777011143319210695L;

	private static String OPEN_TOKEN = "key(";

	private static String CLOSE_TOKEN = ")";

	private DeserializationSchema<Row> instance;

	private String fieldName;

	private int fieldOps;

	private String ruleExpr;

	private Func func;

	public StrengthenDeserializationSchema(DeserializationSchema<Row> instance, String ruleExpr) {
		this.instance = instance;
		this.ruleExpr = ruleExpr;
		this.fieldName = GenericTokenParserUtils.parseFirst(ruleExpr, OPEN_TOKEN, CLOSE_TOKEN);
		this.fieldOps = TypeInformationUtils.parserFieldOps(getProducedType(), fieldName);
		this.func = FuncFactorys.parser(this.ruleExpr);
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return instance.getProducedType();
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		return instance.deserialize(message);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		Object fieldValue = nextElement.getField(fieldOps);
		boolean isEnd = (boolean) func.invoke(Collections.singletonMap(fieldName, fieldValue));
		return isEnd;
	}
}
