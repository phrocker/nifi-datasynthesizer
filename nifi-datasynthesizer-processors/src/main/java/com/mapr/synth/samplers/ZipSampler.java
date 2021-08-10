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
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.nifi.util.StringUtils;
import org.apache.mahout.common.RandomUtils;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Returns data structures containing various aspects of zip codes including location, population and such.
 */
public class ZipSampler extends FieldSampler {
    private JsonNodeFactory nodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private Map<String, List<String>> values = Maps.newHashMap();
    private Set<String> retainedFields = null;
    private Random rand = new Random();
    private int zipCount;
    private double latitudeFuzz = 0;
    private double longitudeFuzz = 0;
    private String country="";
    private LocationBound limits = null;
    private boolean verbose = true;
    private int selectedIndex = -1;

    public ZipSampler() {
        try {
            List<String> names = null;
            //noinspection UnstableApiUsage
            for (String line : Resources.readLines(Resources.getResource("zip.csv"), Charsets.UTF_8)) {
                CsvSplitter onComma = new CsvSplitter();
                if (line.startsWith("#")) {
                    // last comment line contains actual field names
                    names = Lists.newArrayList(onComma.split(line.substring(1)));
                } else {
                    Preconditions.checkState(names != null);
                    Iterable<String> fields = onComma.split(line);
                    Iterator<String> nx = names.iterator();
                    for (String value : fields) {
                        Preconditions.checkState(nx.hasNext());
                        String fieldName = nx.next();
                        List<String> dataList = values.computeIfAbsent(fieldName, k -> Lists.newArrayList());
                        dataList.add(value);
                    }
                    if (!names.iterator().next().equals("V1")) {
                        Preconditions.checkState(!nx.hasNext());
                    }
                    zipCount++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Couldn't read built-in zip code data file", e);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setSeed(long seed) {
        rand = RandomUtils.getRandom(seed);
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setLatitudeFuzz(double fuzz) {
        latitudeFuzz = fuzz;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setCountry(String country) {
        this.country = country;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setLongitudeFuzz(double fuzz) {
        longitudeFuzz = fuzz;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setOnlyContinental(boolean onlyContinental) {
        if (onlyContinental) {
            limits = new BoundingBox(22, 50, -130, -65);
        }
    }

    /**
     * Sets the longitude bounds for the returned points.  The format should be two comma separated decimal numbers
     * representing the minimum and maximum longitude for all returned points.
     *
     * @param bounds A comma separated list of min and max longitude for the returned points.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setLongitude(String bounds) {
        List<Double> boundList = Lists.newArrayList(
                Splitter.on(", ").split(bounds)).stream()
                .map(Double::parseDouble).collect(Collectors.toList());
        Preconditions.checkArgument(boundList.size() == 2);
        double minLongitude = Math.min(boundList.get(0), boundList.get(1));
        double maxLongitude = Math.max(boundList.get(0), boundList.get(1));
        if (limits == null) {
            limits = new BoundingBox(-90, 90, minLongitude, maxLongitude);
        } else {
            Preconditions.checkArgument(limits instanceof BoundingBox);
            ((BoundingBox) limits).setLongitude(minLongitude, maxLongitude);
        }
    }
    @SuppressWarnings("UnusedDeclaration")
    public void setZip(final String zipcode){
        String searchZip = zipcode;
        int idx = zipcode.indexOf("-");
        if (idx > 0){
            searchZip = zipcode.substring(0, idx);
        }
        List<String> sortedZips = Lists.newArrayList( values.get("zip") );
        Collections.sort( sortedZips );
        int zipIndex = Collections.binarySearch(sortedZips, searchZip);
        if (zipIndex < 0 ){
            zipIndex = (-zipIndex - 1);
        }
        // select the closest zip
        final String selectedZip = sortedZips.get(zipIndex);
        OptionalInt indexOpt = IntStream.range(0, values.get("zip").size())
                .filter(i -> selectedZip.equals(values.get("zip").get(i)))
                .findFirst();
        selectedIndex = indexOpt.orElse(-1);
    }

    /**
     * Sets the latitude bounds for the returned points.  The format should be two comma separated decimal numbers
     * representing the minimum and maximum latitude for all returned points.
     *
     * @param bounds A comma separated list of min and max latitude for the returned points.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setLatitude(String bounds) {
        List<Double> boundList = Lists.newArrayList(
                Splitter.on(", ").split(bounds)).stream()
                .map(Double::parseDouble).collect(Collectors.toList());
        Preconditions.checkArgument(boundList.size() == 2);
        double minLatitude = Math.min(boundList.get(0), boundList.get(1));
        double maxLatitude = Math.max(boundList.get(0), boundList.get(1));
        if (limits == null) {
            limits = new BoundingBox(minLatitude, maxLatitude, -180, 180);
        } else {
            Preconditions.checkArgument(limits instanceof BoundingBox);
            ((BoundingBox) limits).setLatitude(minLatitude, maxLatitude);
        }
    }

    /**
     * Sets the center of a radial bound for the returned points.  The format should be two comma separated decimal
     * numbers representing the longitude and latitude of the center of the region.
     *
     * @param bounds A comma separated list of min and max latitude for the returned points.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setNear(String bounds) {
        List<Double> center = Lists.newArrayList(
                Splitter.on(CharMatcher.anyOf(", "))
                        .trimResults().split(bounds)).stream()
                .map(Double::parseDouble).collect(Collectors.toList());
        limits = new RadialBound(center.get(0), center.get(1), 10);
    }

    /**
     * Adjusts the radius for an existing radial bound.  Sets the radius in miles
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setMilesFrom(double distance) {
        Preconditions.checkArgument(limits instanceof RadialBound);
        ((RadialBound) limits).setRadius(distance);
    }

    /**
     * Adjusts the radius for an existing radial bound.  Sets the radius in miles
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setKmFrom(double distance) {
        Preconditions.checkArgument(limits instanceof RadialBound);
        ((RadialBound) limits).setRadius(distance * 0.621371);
    }

    /**
     * Limits the fields that are returned to only those that are specified.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setFields(String fields) {
        Set<String> desiredFields = Sets.newHashSet(Splitter.on(Pattern.compile("[\\s,;]+")).split(fields));
        for (String field : desiredFields) {
            Preconditions.checkArgument(values.containsKey(field), "Invalid field " + field);
        }
        retainedFields = desiredFields;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public JsonNode sample() {
        while (true) {
            int i = selectedIndex >= 0 ? selectedIndex : rand.nextInt(zipCount);
            ObjectNode r = new ObjectNode(nodeFactory);
            for (String key : values.keySet()) {
                r.set(key, new TextNode(values.get(key).get(i)));
            }

            if (!StringUtils.isEmpty(country)){
                r.set("country",new TextNode(country));
            }

            if (latitudeFuzz > 0 || longitudeFuzz > 0) {
                r.set("longitude", new TextNode(String.format("%.4f", r.get("longitude").asDouble() + rand.nextDouble() * longitudeFuzz)));
                r.set("latitude", new TextNode(String.format("%.4f", r.get("latitude").asDouble() + rand.nextDouble() * latitudeFuzz)));
            }

            if (limits == null || limits.accept(r)) {
                if (verbose) {
                    if (retainedFields != null) {
                        for (String key : values.keySet()) {
                            if (!retainedFields.contains(key)) {
                                r.remove(key);
                            }
                        }
                    }
                    return r;
                } else {
                    return r.get("zip");
                }
            }
        }
    }

    @Override
    public void getNames(Set<String> fields) {
        if (isFlat()) {
            if (retainedFields != null) {
                fields.addAll(retainedFields);
            } else {
                fields.addAll(values.keySet());
            }
        } else {
            fields.add(getName());
        }
    }

    private abstract class LocationBound {
        abstract boolean accept(double latitude, double longitude);

        boolean accept(JsonNode location) {
            String longitude = location.get("longitude").asText();
            String latitude = location.get("latitude").asText();
            //noinspection SimplifiableIfStatement
            if (longitude == null || longitude.equals("") || latitude == null || latitude.equals("")) {
                return false;
            } else {
                return accept(Double.parseDouble(latitude), Double.parseDouble(longitude));
            }
        }
    }

    private class BoundingBox extends LocationBound {
        private double minLongitude;
        private double maxLongitude;
        private double minLatitude;
        private double maxLatitude;

        private BoundingBox(double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
            this.minLongitude = Math.min(minLongitude, maxLongitude);
            this.maxLongitude = Math.max(minLongitude, maxLongitude);
            this.minLatitude = Math.min(minLatitude, maxLatitude);
            this.maxLatitude = Math.max(minLatitude, maxLatitude);
        }

        @Override
        boolean accept(double latitude, double longitude) {
            return longitude >= minLongitude && longitude <= maxLongitude && latitude >= minLatitude && latitude <= maxLatitude;
        }

        @SuppressWarnings("WeakerAccess")
        public void setLongitude(double minLongitude, double maxLongitude) {
            this.minLongitude = minLongitude;
            this.maxLongitude = maxLongitude;
        }

        @SuppressWarnings("WeakerAccess")
        public void setLatitude(double minLatitude, double maxLatitude) {
            this.minLatitude = minLatitude;
            this.maxLatitude = maxLatitude;
        }
    }

    private class RadialBound extends LocationBound {
        private static final double EARTH_RADIUS = 3959; // miles
        private final double x;
        private final double y;
        private final double z;

        private double radius;

        private RadialBound(double latitude, double longitude, double radius) {
            x = Math.cos(Math.toRadians(longitude)) * Math.cos(Math.toRadians(latitude));
            y = Math.sin(Math.toRadians(longitude)) * Math.cos(Math.toRadians(latitude));
            z = Math.sin(Math.toRadians(latitude));
            this.radius = 2 * Math.sin(radius / EARTH_RADIUS / 2);
            Preconditions.checkArgument(Math.toDegrees(this.radius) < 70, "Outrageously large radius");
        }

        @Override
        boolean accept(double latitude, double longitude) {
            double x0 = Math.cos(Math.toRadians(longitude)) * Math.cos(Math.toRadians(latitude));
            double y0 = Math.sin(Math.toRadians(longitude)) * Math.cos(Math.toRadians(latitude));
            double z0 = Math.sin(Math.toRadians(latitude));

            double distance = Math.hypot(x0 - x, Math.hypot(y0 - y, z0 - z));
            return distance <= radius;
        }

        public void setRadius(double radius) {
            this.radius = radius / EARTH_RADIUS;
            Preconditions.checkArgument(Math.toDegrees(this.radius) < 70, "Outrageously large radius");
        }
    }

    private class CsvSplitter {
        public Iterable<String> split(final String string) {
            return () -> new Iterator<String>() {
                int max = string.length();
                int position = 0;

                @Override
                public boolean hasNext() {
                    return position <= max;
                }

                private char currentChar() {
                    return string.charAt(position);
                }

                @Override
                public String next() {
                    if (position == max) {
                        position++;
                        return "";
                    } else if (currentChar() == '\"') {
                        position++;
                        int start = position;
                        while (position < max && currentChar() != '\"') {
                            position++;
                        }
                        if (position >= max) {
                            throw new IllegalStateException("Unclosed quoted string");
                        }
                        String r = string.substring(start, position);
                        position += 2;
                        return r;
                    } else {
                        int start = position;
                        while (position < max && currentChar() != ',') {
                            position++;
                        }
                        String r = string.substring(start, position);
                        position++;
                        return r;
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Can't remove from CsvSplitter");
                }
            };
        }
    }
}
