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
import com.fasterxml.jackson.databind.node.IntNode;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Samples from a "foreign key" which is really just an integer.
 * <p>
 * The only cleverness here is that we allow a variable amount of key skew.
 */
public class IdSampler extends FieldSampler {
  private final AtomicInteger current = new AtomicInteger(0);
  private int start;

  public IdSampler() {
  }

  @Override
  public void restart() {
    current.set(this.start);
  }

  @Override
  public JsonNode sample() {
    return new IntNode(current.getAndIncrement());
  }

  @SuppressWarnings("UnusedDeclaration")
  public void setStart(int start) {
    this.start = start;
    this.current.set(start);
  }
}
