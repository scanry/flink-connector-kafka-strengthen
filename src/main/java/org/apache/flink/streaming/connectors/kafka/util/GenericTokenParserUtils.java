package org.apache.flink.streaming.connectors.kafka.util;

import java.util.List;

import org.apache.flink.commons.parser.GenericTokenParser;

/**
 *
 * @author chinacsci
 * @date 2019年9月5日 上午9:12:16
 * @email 359852326@qq.com TODO
 */
public class GenericTokenParserUtils {

	public static String OPEN_TOKEN = "#{";

	public static String CLOSE_TOKEN = "}";

	public static String parseFirst(String text) {
		return parseFirst(text, OPEN_TOKEN, CLOSE_TOKEN);
	}

	public static String parseFirst(String text, String openToken, String closeToken) {
		List<String> result = parser(text, openToken, closeToken);
		return null != result && result.size() > 0 ? result.get(0) : null;
	}

	public static List<String> parser(String text) {
		return parser(text, OPEN_TOKEN, CLOSE_TOKEN);
	}

	public static List<String> parser(String text, String openToken, String closeToken) {
		GenericTokenParser genericTokenParser = new GenericTokenParser(openToken, closeToken, term -> {
			return term;
		});
		genericTokenParser.parse(text);
		return genericTokenParser.getTokens();
	}
}
