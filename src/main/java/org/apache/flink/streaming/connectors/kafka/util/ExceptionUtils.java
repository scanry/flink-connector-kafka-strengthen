package org.apache.flink.streaming.connectors.kafka.util;

/**
 *
 * @author liusong
 * @date 2019年8月19日 上午10:07:02
 * @email 359852326@qq.com TODO
 */
public class ExceptionUtils {

	public static <R> R rethrow(final Throwable throwable) {
		return ExceptionUtils.<R, RuntimeException>typeErasure(throwable);
	}

	@SuppressWarnings("unchecked")
	private static <R, T extends Throwable> R typeErasure(final Throwable throwable) throws T {
		throw (T) throwable;
	}
}
