package org.apache.flink.streaming.connectors.kafka.util;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author: liusong
 * @date: 2018年12月13日
 * @email: 359852326@qq.com
 * @version:
 * @describe: //TODO
 */
public class JsonUtils {

	public static String DEFAULT_DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	public static String DEFAULT_DATE_FORMAT_TIMEZONE = "GMT+8";

	private final static ObjectMapper OBJECT_MAPPER;

	static {
		OBJECT_MAPPER = new ObjectMapper();
		SimpleDateFormat smt = new SimpleDateFormat(DEFAULT_DATE_FORMAT_PATTERN);
		OBJECT_MAPPER.setDateFormat(smt);
		OBJECT_MAPPER.setTimeZone(TimeZone.getTimeZone(DEFAULT_DATE_FORMAT_TIMEZONE));
	}

	public static String toJsonString(Object object) {
		String json = null;
		try {
			json = OBJECT_MAPPER.writeValueAsString(object);
		} catch (Exception exception) {
			ExceptionUtils.rethrow(exception);
		}
		return json;
	}

	public static <T> T toObject(String jsonString, Class<T> clz) {
		T t = null;
		try {
			t = OBJECT_MAPPER.readValue(jsonString, clz);
		} catch (Exception exception) {
			ExceptionUtils.rethrow(exception);
		}
		return t;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> jsonToMap(String jsonString) {
		return toObject(jsonString, Map.class);
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> jsonToLinkedHashMap(String jsonString) {
		return toObject(jsonString, LinkedHashMap.class);
	}

	public static <T> T mapToObject(Map<String, ?> map, Class<T> clz) {
		String json = toJsonString(map);
		T t = null;
		try {
			t = OBJECT_MAPPER.readValue(json, clz);
		} catch (Exception exception) {
			ExceptionUtils.rethrow(exception);
		}
		return t;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> objectToMap(Object ob) {
		if (ob instanceof String) {
			return jsonToMap((String) ob);
		} else {
			return (Map<String, Object>) OBJECT_MAPPER.convertValue(ob, Map.class);
		}
	}

	public static <T> List<T> jsonToList(String text, Class<T> clz) {
		ObjectMapper obMapper = new ObjectMapper();
		JavaType javaType = obMapper.getTypeFactory().constructParametricType(List.class, clz);
		List<T> t = null;
		try {
			t = obMapper.readValue(text, javaType);
		} catch (Exception exception) {
			ExceptionUtils.rethrow(exception);
		}
		return t;
	}
}
