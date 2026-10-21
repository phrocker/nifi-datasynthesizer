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
package org.apache.nifi.datasynthesizer.chaos;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-bound collector of faults injected while one record is being sampled. A processor calls {@link #begin()}
 * before sampling a record and {@link #drain()} afterwards; {@code chaos} samplers reached in between append to it.
 * When no ledger is open, faults are injected but not recorded.
 */
public final class ChaosLedger {

    private static final ThreadLocal<List<FaultRecord>> CURRENT = new ThreadLocal<>();

    private ChaosLedger() {
    }

    public static void begin() {
        CURRENT.set(new ArrayList<>());
    }

    public static void record(final FaultRecord fault) {
        final List<FaultRecord> faults = CURRENT.get();
        if (faults != null) {
            faults.add(fault);
        }
    }

    /** Returns the faults recorded since {@link #begin()} and closes the ledger. */
    public static List<FaultRecord> drain() {
        final List<FaultRecord> faults = CURRENT.get();
        CURRENT.remove();
        return faults == null ? List.of() : faults;
    }

    public static boolean isOpen() {
        return CURRENT.get() != null;
    }
}
