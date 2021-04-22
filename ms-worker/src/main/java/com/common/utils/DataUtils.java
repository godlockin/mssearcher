package com.common.utils;

import com.common.constants.ResultEnum;
import com.exception.MsWorkerException;
import com.google.common.base.CharMatcher;
import com.common.constants.Constants.SysConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.function.BiConsumer;

@Slf4j
public class DataUtils {

    private DataUtils() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    public static String buildKey(String first, String second) {
        return buildKey(first, second, SysConfig.KEY_POSITION);
    }

    public static String buildKey(String first, String second, String delimiter) {
        return String.join(
                Optional.ofNullable(delimiter).orElse(SysConfig.KEY_POSITION)
                , Optional.ofNullable(first).orElse("")
                , Optional.ofNullable(second).orElse("")
        );
    }

    public static MutablePair<String, String> splitKey(String key) {
        return splitKey(key, SysConfig.KEY_POSITION);
    }

    public static MutablePair<String, String> splitKey(String key, String delimiter) {
        if (StringUtils.isBlank(key)) {
            return MutablePair.of("", "");
        }

        String[] items;
        if (!(StringUtils.isNotBlank(delimiter) && key.contains(delimiter))) {
            items = new String[]{ key, "" };
        } else {
            items = key.split(delimiter);
        }

        return MutablePair.of(items[0], items[1]);
    }

    public static <T> T getNotNullValue(Map base, String key, Class<T> clazz, Object defaultValue) {
        return handleNullValue(base.get(key), clazz, defaultValue);
    }

    public static <T> T handleNullValue(Object base, Class<T> clazz, Object defaultValue) {
        return clazz.cast(Optional.ofNullable(base).orElse(defaultValue));
    }

    public static <E> void forEach(Integer maxIndex, Iterable<? extends E> elements, BiConsumer<Integer, ? super E> action) {
        Objects.requireNonNull(elements);
        Objects.requireNonNull(action);
        int index = 0;
        for (E element : elements) {
            action.accept(index++, element);
            if (maxIndex > 0 && maxIndex < index) {
                break;
            }
        }
    }

    public static <T> List<T> handlePaging(int pageIndex, int pageSize, List<T> dataList) {
        int resultSize = dataList.size();
        pageIndex = Math.max(1, pageIndex);
        int start = (pageIndex - 1) * pageSize;
        int end = Math.min(resultSize, pageIndex * pageSize);
        return (start <= end) ? dataList.subList(start, end) : new ArrayList<>();
    }

    public static String removeUselessWhiteSpace(String sentence) {
        if (StringUtils.isBlank(sentence)) {
            return sentence;
        }

        sentence = sentence
                .replaceAll("[\t\r\n]", "")
                .replaceAll("^[　*| *| *|//s*]*", "")
                .replaceAll("[　*| *| *|//s*]*$", "")
                .trim();
        sentence = CharMatcher.anyOf("\r\n\t\u00A0　‭").trimFrom(sentence);

        if (StringUtils.isBlank(sentence)) {
            return sentence;
        }

        int length = sentence.length();
        char[] array = new char[sentence.length()];
        int j = 0;
        char last = sentence.charAt(0);
        array[j] = last;
        for (int i = 1; i < length; i++) {
            char curChar = sentence.charAt(i);
            if (Character.isWhitespace(curChar)) {
                if (isLetterOrDigit(last)) {
                    array[++j] = curChar;
                } else if (i + 1 < length) {
                    char nextChar = sentence.charAt(i + 1);
                    if (isLetterOrDigit(nextChar)) {
                        array[++j] = curChar;
                    }
                }
            } else {
                array[++j] = curChar;
            }
            last = curChar;
        }

        return new String(array, 0, j + 1);
    }

    private static boolean isLetterOrDigit(char c) {
        // upper letter
        // lower letter
        // digit
        return 65 <= c && c <= 90 || 97 <= c && c <= 122 || 48 <= c && c <= 57;
    }
}