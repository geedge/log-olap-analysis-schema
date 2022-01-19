package com.zdjizhi.utils.json;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.http.HttpClientUtil;
import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;

import java.util.*;

/**
 * 使用FastJson解析json的工具类
 *
 * @author qidaijie
 */
public class JsonParseUtil {

    private static final Log logger = LogFactory.get();

    /**
     * 模式匹配，给定一个类型字符串返回一个类类型
     *
     * @param type 类型
     * @return 类类型
     */

    public static Class getClassName(String type) {
        Class clazz;

        switch (type) {
            case "int":
                clazz = Integer.class;
                break;
            case "string":
                clazz = String.class;
                break;
            case "long":
                clazz = long.class;
                break;
            case "array":
                clazz = List.class;
                break;
            case "double":
                clazz = double.class;
                break;
            case "float":
                clazz = float.class;
                break;
            case "char":
                clazz = char.class;
                break;
            case "byte":
                clazz = byte.class;
                break;
            case "boolean":
                clazz = boolean.class;
                break;
            case "short":
                clazz = short.class;
                break;
            default:
                clazz = String.class;
        }
        return clazz;
    }


    /**
     * 获取属性值的方法
     *
     * @param obj      对象
     * @param property key
     * @return 属性的值
     */
    public static Object getValue(Object obj, String property) {

        try {
            BeanMap beanMap = BeanMap.create(obj);
            if (beanMap.containsKey(property)) {
                return beanMap.get(property);
            } else {
                return null;
            }
        } catch (RuntimeException e) {
            logger.error("获取json-value异常，异常key：" + property + "异常信息为：" + e);
            return null;
        }
    }

    /**
     * 获取属性值的方法
     *
     * @param jsonMap  原始日志
     * @param property key
     * @return 属性的值
     */
    public static Object getValue(Map<String, Object> jsonMap, String property) {
        try {
            return jsonMap.getOrDefault(property, null);
        } catch (RuntimeException e) {
            logger.error("获取json-value异常，异常key：" + property + "异常信息为：" + e);
            return null;
        }
    }

    /**
     * long 类型检验转换方法,若为空返回基础值
     *
     * @return Long value
     */
    public static Long getLong(Map<String, Object> jsonMap, String property) {
        Object value = jsonMap.getOrDefault(property, null);
        Long longVal = TypeUtils.castToLong(value);

        if (longVal == null) {
            return 0L;
        }

        return longVal;
    }

    public static String getString(Map<String, Object> jsonMap, String property) {
        Object value = jsonMap.getOrDefault(property, null);
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            return JsonMapper.toJsonString(value);
        }

        if (value instanceof List) {
            return JsonMapper.toJsonString(value);
        }

        return value.toString();
    }

    /**
     * 更新属性值的方法
     *
     * @param jsonMap  原始日志json map
     * @param property 更新的key
     * @param value    更新的值
     */
    public static void setValue(Map<String, Object> jsonMap, String property, Object value) {
        try {
            jsonMap.put(property, value);
        } catch (RuntimeException e) {
            logger.error("赋予实体类错误类型数据", e);
        }
    }

    /**
     * 更新属性值的方法
     *
     * @param obj      对象
     * @param property 更新的key
     * @param value    更新的值
     */
    public static void setValue(Object obj, String property, Object value) {
        try {
            BeanMap beanMap = BeanMap.create(obj);
            beanMap.put(property, value);
        } catch (ClassCastException e) {
            logger.error("赋予实体类错误类型数据", e);
        }
    }

    /**
     * 根据反射生成对象的方法
     *
     * @param properties 反射类用的map
     * @return 生成的Object类型的对象
     */
    public static Object generateObject(Map properties) {
        BeanGenerator generator = new BeanGenerator();
        Set keySet = properties.keySet();
        for (Object aKeySet : keySet) {
            String key = (String) aKeySet;
            generator.addProperty(key, (Class) properties.get(key));
        }
        return generator.create();
    }

    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, String[]> getActionMap() {
        HashMap<String, String[]> map = new HashMap<>(16);

        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> actions = parse.read("$.data.doc.action[*]");

        for (Object action : actions) {
            map.put(JsonPath.read(action, "$.label"),
                    JsonPath.read(action, "$.metrics").toString().split(StreamAggregateConfig.FORMAT_SPLITTER));
//            System.out.println(JsonPath.read(action, "$.label")+JsonPath.read(action, "$.metrics").toString());
        }

        return map;
    }

    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, String[]> getMetricsMap() {
        HashMap<String, String[]> map = new HashMap<>(16);

        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> metrics = parse.read("$.data.doc.metrics[*]");

        for (Object metric : metrics) {
            map.put(JsonPath.read(metric, "$.name"),
                    new String[]{JsonPath.read(metric, "$.function"), JsonPath.read(metric, "$.fieldName")}
            );
        }
        return map;
    }

    /**
     * 获取Metrics内指标，用于过滤原始日志
     *
     * @return 指标列原始名称
     */
    public static ArrayList<String> getLogMetrics() {
        ArrayList<String> list = new ArrayList<>();
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> metrics = parse.read("$.data.doc.metrics[*]");

        for (Object metric : metrics) {
            list.add(JsonPath.read(metric, "$.fieldName"));
        }
        return list;
    }

    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static String getTimeKey() {

        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);

        return JsonPath.read(schema, "$.data.doc.timestamp.name");
    }


    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, Class> getResultLogMap() {
        HashMap<String, Class> map = new HashMap<>(16);

        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> dimensions = parse.read("$.data.doc.dimensions[*]");

        for (Object dimension : dimensions) {
            map.put(JsonPath.read(dimension, "$.name"),
                    JsonParseUtil.getClassName(JsonPath.read(dimension, "$.type")));
        }

        List<Object> metrics = parse.read("$.data.doc.metrics[*]");
        for (Object metric : metrics) {
            map.put(JsonPath.read(metric, "$.name"),
                    JsonParseUtil.getClassName(JsonPath.read(metric, "$.type")));
        }

        return map;
    }

    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, String> getDimensionsMap() {
        HashMap<String, String> map = new HashMap<>(16);

        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> dimensions = parse.read("$.data.doc.dimensions[*]");

        for (Object dimension : dimensions) {
            map.put(JsonPath.read(dimension, "$.name"),
                    JsonPath.read(dimension, "$.fieldName"));
        }

        return map;
    }


    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, String> getFiltersMap() {
        HashMap<String, String> map = new HashMap<>(16);
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> filters = parse.read("$.data.doc.filters[*]");
        for (Object filter : filters) {
            map.put(JsonPath.read(filter, "$.type"), JsonPath.read(filter, "$.fieldName"));
        }

        return map;
    }


    /**
     * 根据http链接获取schema，解析之后返回一个任务列表 (useList toList funcList paramlist)
     *
     * @return 任务列表
     */
    public static ArrayList<String[]> getTransformsList() {
        ArrayList<String[]> list = new ArrayList<>();
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> transforms = parse.read("$.data.doc.transforms[*]");
        for (Object transform : transforms) {
            String function = JsonPath.read(transform, "$.function").toString();
            String name = JsonPath.read(transform, "$.name").toString();
            String fieldName = JsonPath.read(transform, "$.fieldName").toString();
            String parameters = JsonPath.read(transform, "$.parameters").toString();
            list.add(new String[]{function, name, fieldName, parameters});
        }

        return list;
    }

    /**
     * 根据http链接获取schema，解析之后返回一个任务列表 (useList toList funcList paramlist)
     *
     * @return 任务列表
     */
    public static String[] getHierarchy() {
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        DocumentContext parse = JsonPath.parse(schema);
        List<Object> transforms = parse.read("$.data.doc.transforms[*]");
        for (Object transform : transforms) {
            String function = JsonPath.read(transform, "$.function").toString();
            if ("hierarchy".equals(function)) {
                String name = JsonPath.read(transform, "$.name").toString();
                String parameters = JsonPath.read(transform, "$.parameters").toString();
                return new String[]{name, parameters};
            }
        }
        return null;
    }

}