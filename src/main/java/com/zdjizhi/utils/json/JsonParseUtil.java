package com.zdjizhi.utils.json;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import net.sf.cglib.beans.BeanMap;

import java.util.*;
import java.util.concurrent.Executor;

/**
 * 使用FastJson解析json的工具类
 *
 * @author qidaijie
 */
public class JsonParseUtil {

    private static final Log logger = LogFactory.get();
    private static Properties propNacos = new Properties();

    /**
     * 获取actions所有的计算函数
     */
    private static HashMap<String, String[]> actionMap = new HashMap<>(16);

    /**
     * 解析metrics指标字段信息
     */
    private static HashMap<String, String[]> metricFunctionsMap = new HashMap<>(16);

    /**
     * 解析dimensions维度字段信息
     */
    private static HashMap<String, String> dimensionsMap = new HashMap<>(16);

    /**
     * 解析filters过滤信息
     */
    private static HashMap<String, String> filtersMap = new HashMap<>(16);

    /**
     * 解析transforms转换函数信息
     */
    private static ArrayList<String[]> transformsList = new ArrayList<>();

    /**
     * 解析metrics指标字段集
     */
    private static ArrayList<String> metricsFiledNameList = new ArrayList<>();

    /**
     * 解析hierarchy函数，获取切分信息
     */
    private static String[] hierarchy;

    /**
     * 解析时间戳字段名称
     */
    private static String resultTimeKey = "stat_time";

    static {
        propNacos.setProperty(PropertyKeyConst.SERVER_ADDR, StreamAggregateConfig.NACOS_SERVER);
        propNacos.setProperty(PropertyKeyConst.NAMESPACE, StreamAggregateConfig.NACOS_SCHEMA_NAMESPACE);
        propNacos.setProperty(PropertyKeyConst.USERNAME, StreamAggregateConfig.NACOS_USERNAME);
        propNacos.setProperty(PropertyKeyConst.PASSWORD, StreamAggregateConfig.NACOS_PIN);
        try {
            ConfigService configService = NacosFactory.createConfigService(propNacos);
            String dataId = StreamAggregateConfig.NACOS_DATA_ID;
            String group = StreamAggregateConfig.NACOS_GROUP;
            String schema = configService.getConfig(dataId, group, 5000);
            if (StringUtil.isNotBlank(schema)) {
                parseSchema(schema);
            }
            configService.addListener(dataId, group, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configMsg) {
                    if (StringUtil.isNotBlank(configMsg)) {
                        parseSchema(configMsg);
                    }
                }
            });
        } catch (NacosException e) {
            logger.error("Get Schema config from Nacos error,The exception message is :" + e.getMessage());
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
            logger.error("Get the JSON value is abnormal,The key is :" + property + "error message is :" + e);
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
            logger.error("The JSON set value is abnormal,the error message is :", e);
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
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     * 用于反射生成schema类型的对象的一个map集合
     */
    private static void parseSchema(String schema) {
        clearCacheMap();
        DocumentContext parse = JsonPath.parse(schema);

        List<Object> actions = parse.read("$.doc.action[*]");
        for (Object action : actions) {
            actionMap.put(JsonPath.read(action, "$.label"),
                    JsonPath.read(action, "$.metrics").toString().split(StreamAggregateConfig.FORMAT_SPLITTER));
        }

        List<Object> metricFunctions = parse.read("$.doc.metrics[*]");
        for (Object metric : metricFunctions) {
            metricFunctionsMap.put(JsonPath.read(metric, "$.name"),
                    new String[]{JsonPath.read(metric, "$.function"), JsonPath.read(metric, "$.fieldName")}
            );
        }

        List<Object> metrics = parse.read("$.doc.metrics[*]");
        for (Object metric : metrics) {
            metricsFiledNameList.add(JsonPath.read(metric, "$.fieldName"));
        }

        List<Object> dimensions = parse.read("$.doc.dimensions[*]");
        for (Object dimension : dimensions) {
            dimensionsMap.put(JsonPath.read(dimension, "$.name"),
                    JsonPath.read(dimension, "$.fieldName"));
        }

        List<Object> filters = parse.read("$.doc.filters[*]");
        for (Object filter : filters) {
            filtersMap.put(JsonPath.read(filter, "$.type"), JsonPath.read(filter, "$.fieldName"));
        }

        List<Object> transforms = parse.read("$.doc.transforms[*]");
        for (Object transform : transforms) {
            String function = JsonPath.read(transform, "$.function").toString();
            String name = JsonPath.read(transform, "$.name").toString();
            String fieldName = JsonPath.read(transform, "$.fieldName").toString();
            String parameters = JsonPath.read(transform, "$.parameters").toString();
            transformsList.add(new String[]{function, name, fieldName, parameters});
        }

        List<Object> hierarchyList = parse.read("$.doc.transforms[*]");
        for (Object transform : hierarchyList) {
            String function = JsonPath.read(transform, "$.function").toString();
            if ("hierarchy".equals(function)) {
                String name = JsonPath.read(transform, "$.name").toString();
                String parameters = JsonPath.read(transform, "$.parameters").toString();
                hierarchy = new String[]{name, parameters};
            }
        }

        resultTimeKey = JsonPath.read(schema, "$.doc.timestamp.name");
    }

    /**
     * @return 解析schema获取的actions集合
     */
    public static HashMap<String, String[]> getActionMap() {
        return actionMap;
    }

    /**
     * @return 解析schema获取的指标统计方式集合
     */
    public static HashMap<String, String[]> getMetricFunctionsMap() {
        return metricFunctionsMap;
    }

    /**
     * @return 解析schema获取的维度集合
     */
    public static HashMap<String, String> getDimensionsMap() {
        return dimensionsMap;
    }

    /**
     * @return 解析schema获取的过滤规则集合
     */
    public static HashMap<String, String> getFiltersMap() {
        return filtersMap;
    }

    /**
     * @return 解析schema获取的操作集合
     */
    public static ArrayList<String[]> getTransformsList() {
        return transformsList;
    }

    /**
     * @return 解析schema获取的指标字段集合
     */
    public static ArrayList<String> getMetricsFiledNameList() {
        return metricsFiledNameList;
    }

    /**
     * @return 解析schema获取的拆解函数
     */
    public static String[] getHierarchy() {
        return hierarchy;
    }

    /**
     * @return 解析schema获取的时间字段的key
     */
    public static String getResultTimeKey() {
        return resultTimeKey;
    }

    /**
     * 在配置变化时清空缓存，重新解析schema更新缓存
     */
    private static void clearCacheMap() {
        actionMap.clear();
        metricFunctionsMap.clear();
        dimensionsMap.clear();
        filtersMap.clear();
        transformsList.clear();
        metricsFiledNameList.clear();
    }
}