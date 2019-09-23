package org.apache.flink.commons.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 *
 * @author liusong
 * @date 2019年7月10日 下午2:07:05
 * @email 359852326@qq.com TODO
 */

public class GenericTokenParser {

	private final String openToken;
	private final String closeToken;
	private final Function<String, String> tokenHandler;
	private List<String> tokens = new ArrayList<>();
	
	public GenericTokenParser(String openToken, String closeToken,Function<String, String> tokenHandler) {
		this.openToken = openToken;
		this.closeToken = closeToken;
		this.tokenHandler = tokenHandler;
	}

	public String parse(String text) {
		if (text == null || text.isEmpty()) {
			return "";
		}
		int start = text.indexOf(openToken);
		if (start == -1) {
			return text;
		}
		char[] src = text.toCharArray();
		int offset = 0;
		final StringBuilder builder = new StringBuilder();
		StringBuilder expression = null;
		while (start > -1) {
			if (start > 0 && src[start - 1] == '\\') {
				builder.append(src, offset, start - offset - 1).append(openToken);
				offset = start + openToken.length();
			} else {
				if (expression == null) {
					expression = new StringBuilder();
				} else {
					expression.setLength(0);
				}
				builder.append(src, offset, start - offset);
				offset = start + openToken.length();
				int end = text.indexOf(closeToken, offset);
				while (end > -1) {
					if (end > offset && src[end - 1] == '\\') {
						expression.append(src, offset, end - offset - 1).append(closeToken);
						offset = end + closeToken.length();
						end = text.indexOf(closeToken, offset);
					} else {
						expression.append(src, offset, end - offset);
						offset = end + closeToken.length();
						break;
					}
				}
				if (end == -1) {
					builder.append(src, start, src.length - start);
					offset = src.length;
				} else {
					String token=expression.toString();
					tokens.add(token);
					builder.append(tokenHandler.apply(token));
					offset = end + closeToken.length();
				}
			}
			start = text.indexOf(openToken, offset);
		}
		if (offset < src.length) {
			builder.append(src, offset, src.length - offset);
		}
		return builder.toString();
	}

	public List<String> getTokens() {
		return tokens;
	}
}
