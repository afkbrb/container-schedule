package com.tianchi.django.common.utils;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import org.apache.commons.lang3.tuple.ImmutablePair;

import static java.util.stream.Collectors.*;

public class HCollectors {

    public static <A, B> Collector<A, ?, Map<B, Integer>> countingInteger(Function<? super A, ? extends B> classifier) {
        return groupingBy(classifier, reducing(0, e -> 1, Integer::sum));
    }

    public static <A> Collector<A, ?, Map<A, Integer>> countingInteger() {
        return countingInteger(Function.identity());
    }

    public static <K, W> Collector<K, ?, Map<W, K>> objectToKeyMap(Function<K, W> mapper) {
        return toMap(mapper, Function.identity());
    }

    public static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, V>> entriesToMap() {
        return toMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    public static <K, V> Collector<ImmutablePair<K, V>, ?, Map<K, V>> pairToMap() {
        return toMap(pair -> pair.left, pair -> pair.right);
    }
}
