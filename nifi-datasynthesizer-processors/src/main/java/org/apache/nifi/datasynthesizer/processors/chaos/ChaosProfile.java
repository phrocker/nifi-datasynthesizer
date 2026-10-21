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
package org.apache.nifi.datasynthesizer.processors.chaos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.chaos.ChaosLedger;
import org.apache.nifi.datasynthesizer.chaos.Fault;
import org.apache.nifi.datasynthesizer.chaos.FaultRecord;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Record-level chaos configured on a processor. Wraps the record supplier of a batch, applies DUPLICATE, DROP and
 * DRIFT faults, tags every record with a {@code chaos_id} (null when untouched) and collects a manifest of every
 * fault injected, including field-level faults recorded by {@code chaos} samplers in the schema.
 * <p>
 * The manifest is the ground truth for an experiment: a test routes it to a store and asserts that the pipeline
 * under test rejected, quarantined or repaired exactly those records.
 */
public final class ChaosProfile {

    public static final String DEFAULT_ID_FIELD = "chaos_id";

    /** Accepts a probability between 0 and 1 inclusive. */
    static final org.apache.nifi.components.Validator RATE_VALIDATOR = (subject, input, context) -> {
        try {
            final double value = Double.parseDouble(input);
            final boolean ok = value >= 0 && value <= 1;
            return new org.apache.nifi.components.ValidationResult.Builder().subject(subject).input(input).valid(ok)
                    .explanation(ok ? "" : "must be a probability between 0 and 1").build();
        } catch (final NumberFormatException e) {
            return new org.apache.nifi.components.ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("not a number").build();
        }
    };

    public static final PropertyDescriptor CHAOS_ENABLED = new PropertyDescriptor.Builder()
            .name("chaos-enabled")
            .displayName("Chaos Enabled")
            .description("When true, records carry a 'chaos_id' field (null unless the record was tampered with), the "
                    + "record-level rates below apply, and a manifest of every injected fault is routed to 'chaos'. "
                    + "Field-level 'chaos' samplers in the schema corrupt values regardless, but are only recorded "
                    + "in the manifest when this is true.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor CHAOS_DUPLICATE_RATE = new PropertyDescriptor.Builder()
            .name("chaos-duplicate-rate")
            .displayName("Chaos Duplicate Rate")
            .description("Probability (0 to 1) that a record is emitted twice.")
            .defaultValue("0")
            .addValidator(RATE_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CHAOS_DROP_RATE = new PropertyDescriptor.Builder()
            .name("chaos-drop-rate")
            .displayName("Chaos Drop Rate")
            .description("Probability (0 to 1) that a generated record is withheld. It still appears in the manifest, "
                    + "so a consumer can be checked for gap detection.")
            .defaultValue("0")
            .addValidator(RATE_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CHAOS_DRIFT_RATE = new PropertyDescriptor.Builder()
            .name("chaos-drift-rate")
            .displayName("Chaos Drift Rate")
            .description("Probability (0 to 1) that a random field is removed from a record, simulating a producer "
                    + "whose schema drifted.")
            .defaultValue("0")
            .addValidator(RATE_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CHAOS_SEED = new PropertyDescriptor.Builder()
            .name("chaos-seed")
            .displayName("Chaos Seed")
            .description("Seed for the record-level random source so a failing experiment can be replayed. Field-level "
                    + "'chaos' samplers take their own 'seed' in the schema.")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = List.of(
            CHAOS_ENABLED, CHAOS_DUPLICATE_RATE, CHAOS_DROP_RATE, CHAOS_DRIFT_RATE, CHAOS_SEED);

    /** A profile that does nothing, for processors with chaos disabled. */
    public static final ChaosProfile OFF = new ChaosProfile(false, 0, 0, 0, null);

    private final boolean enabled;
    private final double duplicateRate;
    private final double dropRate;
    private final double driftRate;
    private final Random random;

    private ChaosProfile(final boolean enabled, final double duplicateRate, final double dropRate, final double driftRate, final Long seed) {
        this.enabled = enabled;
        this.duplicateRate = duplicateRate;
        this.dropRate = dropRate;
        this.driftRate = driftRate;
        this.random = seed == null ? null : new Random(seed);
    }

    public static ChaosProfile from(final ProcessContext context) {
        if (context.getProperty(CHAOS_ENABLED) == null || !context.getProperty(CHAOS_ENABLED).asBoolean()) {
            return OFF;
        }
        return new ChaosProfile(true,
                context.getProperty(CHAOS_DUPLICATE_RATE).asDouble(),
                context.getProperty(CHAOS_DROP_RATE).asDouble(),
                context.getProperty(CHAOS_DRIFT_RATE).asDouble(),
                context.getProperty(CHAOS_SEED).isSet() ? context.getProperty(CHAOS_SEED).asLong() : null);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** One manifest row. */
    public record ManifestEntry(String chaosId, Fault fault, String field, String original, String injected) {

        public ObjectNode toJson() {
            final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
            node.put(DEFAULT_ID_FIELD, chaosId);
            node.put("fault", fault.name().toLowerCase());
            node.set("field", field == null ? NullNode.getInstance() : TextNode.valueOf(field));
            node.set("original", original == null ? NullNode.getInstance() : TextNode.valueOf(original));
            node.set("injected", injected == null ? NullNode.getInstance() : TextNode.valueOf(injected));
            return node;
        }
    }

    /**
     * A batch-scoped stream of records with chaos applied. Pull with {@link #next()} once per source record; it
     * returns zero, one or two records to write.
     */
    public final class Batch {
        private final Supplier<JsonNode> source;
        private final List<ManifestEntry> manifest = new ArrayList<>();

        private Batch(final Supplier<JsonNode> source) {
            this.source = source;
        }

        public List<ManifestEntry> manifest() {
            return manifest;
        }

        public synchronized List<JsonNode> next() {
            if (!enabled) {
                return List.of(source.get());
            }
            ChaosLedger.begin();
            final JsonNode sampled;
            try {
                sampled = source.get();
            } catch (final RuntimeException e) {
                ChaosLedger.drain();
                throw e;
            }
            final List<FaultRecord> fieldFaults = ChaosLedger.drain();
            if (!(sampled instanceof ObjectNode record)) {
                return List.of(sampled);
            }
            final Random rng = random != null ? random : ThreadLocalRandom.current();
            String chaosId = fieldFaults.isEmpty() ? null : newId();
            for (final FaultRecord fault : fieldFaults) {
                manifest.add(new ManifestEntry(chaosId, fault.fault(), fault.field(), fault.original(), fault.injected()));
            }

            if (rng.nextDouble() < dropRate) {
                chaosId = chaosId == null ? newId() : chaosId;
                manifest.add(new ManifestEntry(chaosId, Fault.DROP, null, summary(record), null));
                return List.of();
            }
            if (rng.nextDouble() < driftRate) {
                final List<String> names = new ArrayList<>();
                record.properties().forEach(entry -> names.add(entry.getKey()));
                if (!names.isEmpty()) {
                    final String removed = names.get(rng.nextInt(names.size()));
                    chaosId = chaosId == null ? newId() : chaosId;
                    manifest.add(new ManifestEntry(chaosId, Fault.DRIFT, removed, describe(record.get(removed)), null));
                    record.remove(removed);
                }
            }
            final boolean duplicate = rng.nextDouble() < duplicateRate;
            if (duplicate) {
                chaosId = chaosId == null ? newId() : chaosId;
                manifest.add(new ManifestEntry(chaosId, Fault.DUPLICATE, null, summary(record), null));
            }
            record.set(DEFAULT_ID_FIELD, chaosId == null ? NullNode.getInstance() : TextNode.valueOf(chaosId));
            return duplicate ? List.of(record, record.deepCopy()) : List.of(record);
        }

        private String newId() {
            return random == null ? UUID.randomUUID().toString() : new UUID(random.nextLong(), random.nextLong()).toString();
        }
    }

    public Batch batch(final Supplier<JsonNode> source) {
        return new Batch(source);
    }

    private static String describe(final JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        final String text = node.isTextual() ? node.asText() : node.toString();
        return text.length() > 256 ? text.substring(0, 253) + "..." : text;
    }

    private static String summary(final ObjectNode record) {
        final ObjectNode copy = record.deepCopy();
        copy.remove(DEFAULT_ID_FIELD);
        return describe(copy);
    }
}
