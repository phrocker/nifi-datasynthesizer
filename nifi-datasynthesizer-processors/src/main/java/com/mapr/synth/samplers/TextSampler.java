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
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.javafaker.Faker;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import com.mapr.synth.FancyTimeFormatter;
import org.apache.mahout.math.random.Multinomial;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Add texts to a context
 *
 * Thread safe for sampling
 */
public class TextSampler extends FieldSampler {
    protected AtomicReference<Multinomial<String>> distribution = new AtomicReference<>();
    protected Random rand = new Random();
    protected List<String> bagOhWords = new ArrayList<>();
    Faker faker = new Faker();
    private enum FakerType{
        CHUCK_NORRIS,
        BACK_TO_THE_FUTURE,
        GAME_OF_THRONES
    }
    FakerType myType = FakerType.BACK_TO_THE_FUTURE;
    public TextSampler() {
    }

    @SuppressWarnings("unused")
    public void setText(String string) {
        bagOhWords.add(string);
    }

    @SuppressWarnings("unused")
    public void setType(String type) {
        if ("chucknorris".equalsIgnoreCase(type)){
            myType = FakerType.CHUCK_NORRIS;
        }
        else if("backtothefuture".equalsIgnoreCase(type)){
            myType = FakerType.BACK_TO_THE_FUTURE;
        }
        else if("gameofthrones".equalsIgnoreCase(type)){
            myType = FakerType.GAME_OF_THRONES;
        }

    }


    public TextSampler(String resource) {

    }

    private String getString(){
        if ( bagOhWords.isEmpty()){
            switch(myType){
                case CHUCK_NORRIS:
                    return faker.chuckNorris().fact();
                case BACK_TO_THE_FUTURE:
                    return faker.backToTheFuture().quote();
                case GAME_OF_THRONES:
                    return faker.gameOfThrones().quote();
            }
        }

        return bagOhWords.get( rand.nextInt(bagOhWords.size()));
    }

    @Override
    public JsonNode sample() {
      synchronized (this) {
        return new TextNode( getString() );
      }
    }
}
