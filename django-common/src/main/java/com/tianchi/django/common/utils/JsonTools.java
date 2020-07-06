package com.tianchi.django.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.fastjson.serializer.SerializerFeature.*;
import static com.google.common.base.CharMatcher.whitespace;

@Slf4j
public class JsonTools {

    static {
        JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    }

    private static final SerializerFeature[] jsonFeatures = new SerializerFeature[] {
        SkipTransientField,
        WriteDateUseDateFormat,
        DisableCircularReferenceDetect,
        PrettyFormat
        //WriteNullListAsEmpty
        //WriteClassName
    };

    /**
     * 将对象输出为可反序列化的json字符串，以用于数据存储和传输
     */
    public static <T> String toJson(T t) {
        return JSON.toJSONString(t, jsonFeatures);
    }

    public static <T> String toSimplifyJson(T t) {
        return whitespace().removeFrom(toJson(t));
    }

    /**
     * 将json字符串转换为指定类型的对象
     */
    public static <T> T parse(String json, Class<T> clazz) {
        try {
            return JSON.parseObject(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 通过TypeReference将json字符串转换为特定类型，常用语list<string>类型转换
     */
    public static <T> T parse(String json, TypeReference<T> typeReference) {
        try {
            return JSON.parseObject(json, typeReference);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}