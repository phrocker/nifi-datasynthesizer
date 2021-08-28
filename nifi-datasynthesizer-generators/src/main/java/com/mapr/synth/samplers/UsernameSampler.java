/*
 * Licensed to the Ted Dunning under one or more contributor license
 * agreements.  See the NOTICE file that may be
 * distributed with this work for additional information
 * regarding copyright ownership.  Ted Dunning licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.mapr.synth.samplers;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.mahout.math.random.Multinomial;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Samples a version 4 (random) UUID.  Note that the random bits generated are pull from the
 * standard Java random number generator and are subject to limitations because of that.
 *
 * See http://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29
 *
 * Thread safe.
 */

public class UsernameSampler extends StringSampler {
    private final Random rand = new SecureRandom();
    public UsernameSampler() {
        init("names.txt");
    }
    protected void init(String resourceName) {
        try {
            if (distribution.compareAndSet(null, new Multinomial<>())) {

                double i = 20;
                for (String line : Resources.readLines(Resources.getResource(resourceName), Charsets.UTF_8)) {
                    if (!line.startsWith("#")) {
                        String name = translate(line);
                        double weight = 1.0 / i;
                        distribution.get().add(name, weight);
                    }
                    i++;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Couldn't read built-in resource file", e);
        }
    }

    public static void main(String [] args){
        UsernameSampler sampler = new UsernameSampler();
    }
}
