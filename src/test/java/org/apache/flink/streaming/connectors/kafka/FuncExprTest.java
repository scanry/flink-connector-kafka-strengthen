package org.apache.flink.streaming.connectors.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.chinacscs.basicplatform.commons.expr.FuncFactorys;

/**
 *
 * @author chinacsci
 * @date   2019年9月6日 下午4:30:47
 * @email  359852326@qq.com
 * TODO
 */
public class FuncExprTest {

	public static void main(String[] args){
		String expr="count(key(list))";
		List<Map<String, Object>> datas=new ArrayList<>();
		Map<String, Object> data=null;
		
		data=new HashMap<>();
		data.put("scroe", 1);
		datas.add(data);
		
		data=new HashMap<>();
		data.put("scroe", 2);
		datas.add(data);
		
		data=new HashMap<>();
		data.put("scroe", 3);
		datas.add(data);
		
		data=new HashMap<>();
		data.put("scroe", -3);
		datas.add(data);
		
		Map<String, Object> context=new HashMap<>();
		context.put("list",datas);
		Object result=FuncFactorys.invoke(expr, context);
		System.out.println(result);
		
		System.out.println(say("hi",(s1)->s1));
	}
	
	public static String say(String text,Function<String,String> func) {
		return func.apply(text);
	}
}
