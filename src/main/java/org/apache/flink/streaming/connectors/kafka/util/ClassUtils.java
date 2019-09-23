package org.apache.flink.streaming.connectors.kafka.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author liusong
 * @date 2019年7月25日 上午10:38:59
 * @email 359852326@qq.com TODO
 */

public class ClassUtils {

	public static Object newInstance(String className) {
		try {
			Class<?> clz = Class.forName(className);
			return clz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List<Method> findMethods(String className, String methodName) {
		try {
			Class<?> clz = Class.forName(className);
			return findMethods(clz, methodName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List<Method> findMethods(Class<?> clz, String methodName) {
		try {
			Method[] methods = clz.getMethods();
			List<Method> findMethods = new ArrayList<Method>();
			for (Method method : methods) {
				if (method.getName().equals(methodName)) {
					findMethods.add(method);
				}
			}
			return findMethods;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Method selectMethods(Class<?> clz, String methodName, Object[] args) {
		try {
			Method[] methods = clz.getMethods();
			Method matchMethod = null;
			for (Method method : methods) {
				if (method.getName().equals(methodName) && matchMethod(method, args)) {
					matchMethod = method;
					break;
				}
			}
			if (null == matchMethod) {
				throw new RuntimeException();
			}
			return matchMethod;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Object invoke(Object instance, String methodName, Object[] args) {
		try {
			Method targetMethod = selectMethods(instance.getClass(), methodName, args);
			return targetMethod.invoke(instance, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean matchMethod(Method method, Object[] args) {
		if (method.getParameterTypes().length != args.length) {
			return false;
		}
		boolean matchResult = true;
		Class<?>[] parameterTypes = method.getParameterTypes();
		for (int i = 0; i < args.length; i++) {
			Object arg = args[i];
			Class<?> argClz = arg.getClass();
			if (null != args && !equalsClz(parameterTypes[i], argClz)) {
				matchResult = false;
				break;
			}
		}
		return matchResult;
	}

	private static Map<Class<?>,Class<?>> clzMap=new HashMap<>();
	
	static {
		clzMap.put(byte.class, byte.class);
		clzMap.put(Byte.class, byte.class);
		
		clzMap.put(boolean.class, boolean.class);
		clzMap.put(Boolean.class, boolean.class);
		
		clzMap.put(short.class, short.class);
		clzMap.put(Short.class, short.class);
		
		clzMap.put(char.class, char.class);
		clzMap.put(Character.class, char.class);
		
		clzMap.put(int.class, int.class);
		clzMap.put(Integer.class, int.class);
		
		clzMap.put(long.class, long.class);
		clzMap.put(Long.class, long.class);
		
		clzMap.put(double.class, double.class);
		clzMap.put(Double.class, double.class);
		
		clzMap.put(float.class, float.class);
		clzMap.put(Float.class, float.class);
	}
	
	private static boolean equalsClz(Class<?> clz1, Class<?> clz2) {
		Class<?> clz1Proxy=clzMap.getOrDefault(clz1, clz1);
		Class<?> clz2Proxy=clzMap.getOrDefault(clz2, clz2);
		return clz1Proxy.equals(clz2Proxy);
	}
}
