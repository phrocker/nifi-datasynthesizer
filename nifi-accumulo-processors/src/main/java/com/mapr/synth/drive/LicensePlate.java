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

package com.mapr.synth.drive;

public class LicensePlate {
    static String[] INVALID_PLATE_LETTERS = { "FOR", "AXE", "JAM", "JAB", "ZIP", "ARE", "YOU",
            "JUG", "JAW", "JOY" };

    static String generateLetters(int amount) {
        String letters = "";
        int n = 'Z' - 'A' + 1;
        for (int i = 0; i < amount; i++) {
            char c = (char) ('A' + Math.random() * n);
            letters += c;
        }
        return letters;
    }

    static String generateDigits(int amount) {
        String digits = "";
        int n = '9' - '0' + 1;
        for (int i = 0; i < amount; i++) {
            char c = (char) ('0' + Math.random() * n);
            digits += c;
        }
        return digits;
    }

    public static String generateLicensePlate() {
        String licensePlate;
        String letters;
        do {
            letters = generateLetters(3);
        } while (illegalWord(letters));

        String digits = generateDigits(3);

        licensePlate = letters + "-" + digits;
        return licensePlate;
    }

    private static boolean illegalWord(String letters) {
        for (int i = 0; i < INVALID_PLATE_LETTERS.length; i++) {
            if (letters.equals(INVALID_PLATE_LETTERS[i])) {
                return true;
            }
        }
        return false;
    }

    public static void main(String args[]) {

        System.out.println(generateLicensePlate());
    }
}

