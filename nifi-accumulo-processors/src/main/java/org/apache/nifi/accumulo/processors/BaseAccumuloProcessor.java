/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.nifi.accumulo.processors;

import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Base Accumulo class that provides connector services, table name, and thread
 * properties
 */
public abstract class BaseAccumuloProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Instance Name")
            .description("Instance name of the Accumulo cluster")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    protected static final PropertyDescriptor ACCUMULO_USER = new PropertyDescriptor.Builder()
            .name("Accumulo User")
            .description("Connecting user for Accumulo")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor ACCUMULO_PASSWORD = new PropertyDescriptor.Builder()
            .name("Accumulo Password")
            .description("Connecting user's password when using the PASSWORD Authentication type")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table into which data will be placed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("Create Table")
            .description("Creates a table if it does not exist. This property will only be used when EL is not present in 'Table Name'")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    protected static final PropertyDescriptor THREADS = new PropertyDescriptor.Builder()
            .name("Threads")
            .description("Number of threads used for reading and writing")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    /**
     * Implementations can decide to include all base properties or individually include them. List is immutable
     * so that implementations must constructor their own lists knowingly
     */

    protected static final ImmutableList<PropertyDescriptor> baseProperties = ImmutableList.of(ZOOKEEPER_QUORUM, INSTANCE_NAME, ACCUMULO_USER, ACCUMULO_PASSWORD,TABLE_NAME,CREATE_TABLE,THREADS);

    AccumuloClient getClient(final ProcessContext context){
        final String instanceName = context.getProperty(INSTANCE_NAME).evaluateAttributeExpressions().getValue();
        final String zookeepers = context.getProperty(ZOOKEEPER_QUORUM).evaluateAttributeExpressions().getValue();
        final String accumuloUser = context.getProperty(ACCUMULO_USER).evaluateAttributeExpressions().getValue();

        final AuthenticationToken token = new PasswordToken(context.getProperty(ACCUMULO_PASSWORD).getValue());

        return Accumulo.newClient().to(instanceName,zookeepers).as(accumuloUser,token).build();
    }

}
