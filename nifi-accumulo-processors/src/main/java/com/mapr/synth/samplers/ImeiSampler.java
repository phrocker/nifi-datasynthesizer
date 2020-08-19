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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import com.google.common.io.Resources;

import org.apache.mahout.common.RandomUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Samples from Social Security Numbers with roughly equal representation across
 * different ages
 */
public class ImeiSampler extends FieldSampler {

    private Random rand = RandomUtils.getRandom();
    private BloomFilter<String> bloomy = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 20000000L);
    private boolean unique = false;

    public ImeiSampler() {
        
    }

    @Override
    @SuppressWarnings("unused")
    public void setSeed(long seed) {
        rand = new Random(seed);
    }

    /**
     * Limits the fields that are returned to only those that are specified.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setFields(String fields) {
        
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setTypes(String types) {
        Set<String> keepTypes = Sets.newHashSet(Splitter.on(Pattern.compile("[\\s,;]+")).split(types));
        for (String type : keepTypes) {
            if (type.equals("unique"))
                unique = true;
        }
    }


    @Override
    public JsonNode sample() {

        if (unique) {
            
            return new TextNode(generateUniqueImei());
            
        }
        else{
            return new TextNode(generateIMEI());
        }
    }

    private String generateUniqueImei(){
        String imei = "";
        do{
            imei = generateIMEI();
        }while( bloomy.mightContain(imei));
        bloomy.put(imei);
        return imei;
    }

    // found at https://gist.github.com/abforce/c9a2dabdbe7fab51d7485deeddb67876
    private String generateIMEI() {
        int pos;
        int[] str = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int sum = 0;
        int final_digit;
        int t;
        int len_offset;
        int len = 15;
        String imei = "";

        String[] rbi = new String[]{"01", "10", "30", "33", "35", "44", "45", "49", "50", "51", "52", "53", "54", "86", "91", "98", "99"};
        String[] arr = rbi[(int) Math.floor(Math.random() * rbi.length)].split("");
        str[0] = Integer.parseInt(arr[0]);
        str[1] = Integer.parseInt(arr[1]);
        pos = 2;

        while (pos < len - 1) {
            str[pos++] = (int) (Math.floor(Math.random() * 10) % 10);
        }

        len_offset = (len + 1) % 2;
        for (pos = 0; pos < len - 1; pos++) {
            if ((pos + len_offset) % 2 != 0) {
                t = str[pos] * 2;
                if (t > 9) {
                    t -= 9;
                }
                sum += t;
            } else {
                sum += str[pos];
            }
        }

        final_digit = (10 - (sum % 10)) % 10;
        str[len - 1] = final_digit;

        for (int d : str) {
            imei += String.valueOf(d);
        }

        return imei;
    }
    
}
