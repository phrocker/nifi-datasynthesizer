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
package org.apache.nifi.datasynthesizer.samplers;

import com.fasterxml.jackson.databind.JsonNode;
import com.mapr.synth.samplers.FieldSampler;
import org.apache.nifi.datasynthesizer.chaos.ChaosLedger;
import org.apache.nifi.datasynthesizer.chaos.Fault;
import org.apache.nifi.datasynthesizer.chaos.FaultInjector;
import org.apache.nifi.datasynthesizer.chaos.FaultRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Wraps any sampler and corrupts its output at a configured rate, recording each fault in the {@link ChaosLedger}
 * so the processor can emit a manifest of what was injected.
 * <p>
 * Schema example:
 * <pre>
 * {"name": "email", "class": "chaos", "rate": 0.05, "faults": ["null", "unicode", "injection"], "seed": 42,
 *  "value": {"class": "email"}}
 * </pre>
 * {@code rate} is the probability (0 to 1) that a sample is corrupted; {@code faults} is the subset of
 * {@link Fault#FIELD_FAULTS} to draw from (all by default); {@code seed} makes the run reproducible;
 * {@code oversize} is the length used by the OVERSIZE fault (default 8192).
 * <p>
 * Thread safe.
 */
public class ChaosSampler extends FieldSampler {

    private FieldSampler value;
    private double rate = 0.05;
    private List<Fault> faults = Fault.FIELD_FAULTS;
    private Random random = null;
    private int oversize = 8192;

    public ChaosSampler() {
    }

    @SuppressWarnings("unused")
    public void setValue(final JsonNode definition) {
        value = FieldSampler.newSampler(definition);
    }

    @SuppressWarnings("unused")
    public void setRate(final double rate) {
        if (rate < 0 || rate > 1) {
            throw new IllegalArgumentException("rate must be between 0 and 1");
        }
        this.rate = rate;
    }

    @SuppressWarnings("unused")
    public void setFaults(final List<String> names) {
        final List<Fault> parsed = new ArrayList<>();
        for (final String name : names) {
            final Fault fault = Fault.parse(name);
            if (fault.isRecordLevel()) {
                throw new IllegalArgumentException(fault + " is a record-level fault; configure it on the processor");
            }
            parsed.add(fault);
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("faults must not be empty");
        }
        faults = List.copyOf(parsed);
    }

    @SuppressWarnings("unused")
    public void setSeed(final long seed) {
        random = new Random(seed);
    }

    @SuppressWarnings("unused")
    public void setOversize(final int oversize) {
        this.oversize = Math.max(1, oversize);
    }

    @Override
    public void getNames(final Set<String> fields) {
        if (value != null && isFlat()) {
            value.getNames(fields);
        } else {
            super.getNames(fields);
        }
    }

    /** Forwards to the wrapped sampler so sequences restart as they would without chaos; the chaos source is untouched. */
    @Override
    public void restart() {
        if (value != null) {
            value.restart();
        }
    }

    @Override
    public JsonNode sample() {
        if (value == null) {
            throw new IllegalStateException("chaos sampler '" + getName() + "' requires a 'value' sampler to wrap");
        }
        final JsonNode clean = value.sample();
        synchronized (this) {
            final Random rng = random != null ? random : ThreadLocalRandom.current();
            if (rng.nextDouble() >= rate) {
                return clean;
            }
            final Fault fault = faults.get(rng.nextInt(faults.size()));
            // Faults draw a value-dependent number of random numbers; give each its own derived source so the
            // corrupt-or-not decision stream stays identical for a seed regardless of the wrapped values.
            final JsonNode corrupted = FaultInjector.apply(fault, clean, new Random(rng.nextLong()), oversize);
            ChaosLedger.record(new FaultRecord(fault, getName(), describe(clean), describe(corrupted)));
            return corrupted;
        }
    }

    private static String describe(final JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        final String text = node.isTextual() ? node.asText() : node.toString();
        return text.length() > 256 ? text.substring(0, 253) + "..." : text;
    }
}
