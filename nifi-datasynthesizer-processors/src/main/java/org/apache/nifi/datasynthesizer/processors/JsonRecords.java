/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.datasynthesizer.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

/**
 * Converts the Jackson trees that log-synth samplers produce straight into NiFi records, without serializing to
 * JSON text and parsing it back. Schemas are inferred with NiFi's own JSON schema inference so date, time and
 * timestamp strings get typed the same way the JsonTreeReader would type them.
 */
public final class JsonRecords {

    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final TimeValueInference TIME_INFERENCE = new TimeValueInference(DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

    private JsonRecords() {
    }

    /** Infers a record schema from one or more sample objects. */
    public static RecordSchema inferSchema(final JsonNode... samples) throws IOException {
        final Queue<JsonNode> queue = new ArrayDeque<>(Arrays.asList(samples));
        return new JsonSchemaInference(TIME_INFERENCE).inferSchema(queue::poll);
    }

    /** Infers the data type a value would have as a field of a record. */
    public static DataType inferDataType(final String fieldName, final JsonNode value) throws IOException {
        final ObjectNode wrapper = new ObjectNode(JsonNodeFactory.instance);
        wrapper.set(fieldName, value);
        return inferSchema(wrapper).getDataType(fieldName).orElse(RecordFieldType.STRING.getDataType());
    }

    /** Returns a schema with all fields of {@code base} followed by one extra field. */
    public static RecordSchema withField(final RecordSchema base, final String fieldName, final DataType dataType) {
        final List<RecordField> fields = new ArrayList<>(base.getFields());
        fields.removeIf(f -> f.getFieldName().equals(fieldName));
        fields.add(new RecordField(fieldName, dataType, true));
        return new SimpleRecordSchema(fields);
    }

    /** Converts an object node into a record of the given schema. Fields absent from the schema are kept untyped. */
    public static Record toRecord(final JsonNode node, final RecordSchema schema) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonNode> entry : node.properties()) {
            final DataType dataType = schema.getDataType(entry.getKey()).orElse(null);
            values.put(entry.getKey(), toValue(entry.getValue(), dataType));
        }
        return new MapRecord(schema, values, false, false);
    }

    /** Converts a JSON value into the Java value NiFi record writers expect for the given data type. */
    public static Object toValue(final JsonNode node, final DataType declared) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        final DataType dataType = declared instanceof ChoiceDataType choice ? choose(choice, node) : declared;
        if (node.isObject()) {
            final RecordSchema child = Optional.ofNullable(dataType)
                    .filter(RecordDataType.class::isInstance)
                    .map(dt -> ((RecordDataType) dt).getChildSchema())
                    .orElseGet(() -> inferQuietly(node));
            return toRecord(node, child);
        }
        if (node.isArray()) {
            final DataType element = dataType instanceof ArrayDataType array ? array.getElementType() : null;
            final Object[] values = new Object[node.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = toValue(node.get(i), element);
            }
            return values;
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        final String text = node.asText();
        if (dataType != null && isTemporal(dataType)) {
            // Writers convert with their own configured patterns, so hand them temporal objects, not strings.
            final Optional<String> format = Optional.ofNullable(dataType.getFormat());
            try {
                return DataTypeUtils.convertType(text, dataType, format, format, format, "value");
            } catch (final RuntimeException e) {
                return text;
            }
        }
        return text;
    }

    private static boolean isTemporal(final DataType dataType) {
        return switch (dataType.getFieldType()) {
            case DATE, TIME, TIMESTAMP -> true;
            default -> false;
        };
    }

    private static DataType choose(final ChoiceDataType choice, final JsonNode node) {
        for (final DataType candidate : choice.getPossibleSubTypes()) {
            if (node.isObject() && candidate instanceof RecordDataType) {
                return candidate;
            }
            if (node.isArray() && candidate instanceof ArrayDataType) {
                return candidate;
            }
        }
        return choice;
    }

    private static RecordSchema inferQuietly(final JsonNode node) {
        try {
            return inferSchema(node);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
