package com.zdjizhi.utils.json;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.exception.AnalysisException;

import java.util.List;
import java.util.Map;

/**
 * @author qidaijie
 * @Package PACKAGE_NAME
 * @Description:
 * @date 2021/7/1217:34
 */
public class JsonTypeUtils {
    private static final Log logger = LogFactory.get();
    /**
     * String 类型检验转换方法
     *
     * @param value json value
     * @return String value
     */
    public static String checkString(Object value) {
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
     * array 类型检验转换方法
     *
     * @param value json value
     * @return List value
     */
    private static Map checkObject(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            return (Map) value;
        }

        throw new AnalysisException("can not cast to map, value : " + value);
    }

    /**
     * array 类型检验转换方法
     *
     * @param value json value
     * @return List value
     */
    private static List checkArray(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            return (List) value;
        }

        throw new AnalysisException("can not cast to List, value : " + value);
    }

    private static Long checkLong(Object value) {
        if (value == null) {
            return null;
        }

        return TypeUtils.castToLong(value);
    }

    /**
     * long 类型检验转换方法,若为空返回基础值
     *
     * @param value json value
     * @return Long value
     */
    public static long checkLongValue(Object value) {

        Long longVal = TypeUtils.castToLong(value);

        if (longVal == null) {
            return 0L;
        }

        return longVal;
    }

    /**
     * Double 类型校验转换方法
     *
     * @param value json value
     * @return Double value
     */
    private static Double checkDouble(Object value) {
        if (value == null) {
            return null;
        }

        return TypeUtils.castToDouble(value);
    }


    private static Integer checkInt(Object value) {
        if (value == null) {
            return null;
        }

        return TypeUtils.castToInt(value);
    }


    /**
     * int 类型检验转换方法,若为空返回基础值
     *
     * @param value json value
     * @return int value
     */
    private static int getIntValue(Object value) {

        Integer intVal = TypeUtils.castToInt(value);

        if (intVal == null) {
            return 0;
        }
        return intVal;
    }

}
