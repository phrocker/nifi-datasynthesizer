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

import com.mapr.synth.samplers.SchemaSampler;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.SamplerBase;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"data-synthesis", "synthetic", "record", "log-synth", "generate", "test"})
@CapabilityDescription("Generates records from a log-synth schema. The schema can be set on the processor, looked up by "
        + "name from a Schema Lookup Service, or supplied as the content of an incoming FlowFile. Fields of class 'llm' "
        + "are filled by the configured Text Generation Service.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records written"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the record writer")})
public class DataSynthesizer extends DataSynthesizerBase {

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Incoming FlowFiles whose content was used as the schema")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = withChaos(List.of(
            SCHEMA, SCHEMA_LOOKUP_SERVICE, RECORD_COUNT, RECORD_WRITER, TEXT_GENERATION_SERVICE));
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL, REL_CHAOS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final boolean connected = context.hasIncomingConnection();
        final FlowFile input = connected ? session.get() : null;
        if (connected && input == null) {
            // Another concurrent task took the queued FlowFile; generating here would emit an unparented batch.
            return;
        }
        if (!connected && !hasConfiguredSchema()) {
            getLogger().warn("No schema configured and no incoming connection; nothing to generate");
            context.yield();
            return;
        }
        final int recordCount = context.getProperty(RECORD_COUNT).asInteger();
        try (TextCompletionContext.Scope ignored = bindTextGeneration()) {
            final SamplerBase sampler;
            if (hasConfiguredSchema()) {
                sampler = configuredSampler();
            } else {
                final String schemaText;
                try (InputStream in = session.read(input)) {
                    schemaText = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
                final SchemaSampler parsed = parseSchema(schemaText);
                sampler = () -> Optional.of(parsed.sample());
            }
            final FlowFile output = writeRecords(session, context, input, () -> next(sampler), recordCount);
            session.transfer(output, REL_SUCCESS);
            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        } catch (final IOException | RuntimeException e) {
            if (input == null) {
                getLogger().error("Failed to synthesize records", e);
                context.yield();
                throw new ProcessException("Failed to synthesize records: " + e.getMessage(), e);
            }
            getLogger().error("Failed to synthesize records for {}", input, e);
            session.transfer(session.penalize(input), REL_FAILURE);
        }
    }
}
