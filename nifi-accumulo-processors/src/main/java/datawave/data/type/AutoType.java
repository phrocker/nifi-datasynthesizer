package datawave.data.type;

import java.lang.reflect.InvocationTargetException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import datawave.data.type.util.NumericalEncoder;

public class AutoType {
    private static final Pattern diacriticals = Pattern.compile("\\p{InCombiningDiacriticalMarks}");

    static String removeDiacriticalMarks(String str) {
        Matcher matcher = diacriticals.matcher(str);
        return matcher.replaceAll("");
    }

    static String normalize(String className, String value) {
        if (className.endsWith("NumberType") || className.endsWith("GeoLatType") || className.endsWith("GeoLonType"))
        {
            try {
                return NumericalEncoder.encode(value);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to normalize value as a number: " + value);
            }
        }
        return removeDiacriticalMarks(value);
    }
}