package org.apache.flink.streaming.connectors.kafka.util;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.types.Row;

import lombok.Getter;

/**
 *
 * @author chinacsci
 * @date 2019年9月4日 下午2:59:30
 * @email 359852326@qq.com TODO
 */
public class TypeInformationUtils {

	private static final String REGEX_FIELD = "[\\p{L}\\p{Digit}_\\$]*"; // This can start with a digit (because of
	// Tuples)
	private static final String REGEX_NESTED_FIELDS = "(" + REGEX_FIELD + ")(\\.(.+))?";
	private static final String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS + "|\\"
			+ Keys.ExpressionKeys.SELECT_ALL_CHAR + "|\\" + Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	private static ExecutionConfig EXECUTION_CONFIG = new ExecutionConfig();

	public static int parserFieldOps(TypeInformation<Row> typeInfo, String field) {
		return parserFieldOps(typeInfo, field, EXECUTION_CONFIG);
	}

	@SuppressWarnings("rawtypes")
	public static int parserFieldOps(TypeInformation<Row> typeInfo, String field, ExecutionConfig config) {
		int fieldIndex = -1;
		if (typeInfo instanceof BasicTypeInfo) {
			fieldIndex = field.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR) ? 0 : Integer.parseInt(field);
		} else if (typeInfo instanceof PojoTypeInfo) {
			FieldExpression decomp = decomposeFieldExpression(field);
			PojoTypeInfo<?> pojoTypeInfo = (PojoTypeInfo) typeInfo;
			fieldIndex = pojoTypeInfo.getFieldIndex(decomp.head);
		} else if (typeInfo.isTupleType() && ((TupleTypeInfoBase) typeInfo).isCaseClass()) {
			TupleTypeInfoBase tupleTypeInfo = (TupleTypeInfoBase) typeInfo;
			FieldExpression decomp = decomposeFieldExpression(field);
			fieldIndex = tupleTypeInfo.getFieldIndex(decomp.head);
		} else if (typeInfo.isTupleType()) {
			TupleTypeInfoBase tupleTypeInfo = (TupleTypeInfoBase) typeInfo;
			FieldExpression decomp = decomposeFieldExpression(field);
			fieldIndex = tupleTypeInfo.getFieldIndex(decomp.head);
		}
		if (fieldIndex == -1) {
			throw new CompositeType.InvalidFieldReferenceException(
					"Unable to find field \"" + field + "\" in type " + typeInfo + ".");
		}
		return fieldIndex;
	}

	private static FieldExpression decomposeFieldExpression(String fieldExpression) {
		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if (!matcher.matches()) {
			throw new CompositeType.InvalidFieldReferenceException(
					"Invalid field expression \"" + fieldExpression + "\".");
		}
		String head = matcher.group(0);
		if (head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR)
				|| head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			throw new CompositeType.InvalidFieldReferenceException("No wildcards are allowed here.");
		} else {
			head = matcher.group(1);
		}
		String tail = matcher.group(3);
		return new FieldExpression(head, tail);
	}

	@Getter
	private static class FieldExpression implements Serializable {

		private static final long serialVersionUID = 1L;

		public String head, tail; // tail can be null, if the field expression had just one part

		FieldExpression(String head, String tail) {
			this.head = head;
			this.tail = tail;
		}
	}
}
