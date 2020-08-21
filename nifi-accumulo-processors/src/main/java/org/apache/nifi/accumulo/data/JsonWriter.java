package org.apache.nifi.accumulo.data;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.NullSuppression;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyOutputStream;

public class JsonWriter {

    static  NullSuppression nullSuppression=NullSuppression.SUPPRESS_MISSING;
    static OutputGrouping outputGrouping=OutputGrouping.OUTPUT_ARRAY;

    public static RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {

        final OutputStream bufferedOut = new BufferedOutputStream(out, 65536);
        final OutputStream compressionOut;
        String mimeTypeRef= "application/json";
                    compressionOut = out;

        return new org.apache.nifi.json.WriteJsonResult(logger, schema, new NopSchemaAccessWriter(), compressionOut, false, nullSuppression, outputGrouping,
                null,null,null, mimeTypeRef);
    }

}
