<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# NiFi DataSynthesizer

This is a basic data synthesizer NAR which utilizes [log-synth](https://github.com/tdunning/log-synth) and [Java Faker](https://github.com/DiUS/java-faker) to generate semi-realistic data within records.

The package contains the following processors:

Processor Name | Description
------------ | -------------
DataCorrelator | Data correlator processor that joins two generated records with a free-form schema for a new record that will be emplaced within the input record
DataSynthesizer | Basic data synthesizer that supports a schema ala log-synth. These Json schemas support a variety of generators. Use this for free-form generation
CommuterData | Generates commuter data, simulating the drive of a daily commuter. Can specify a home location ( as a zip code ) to center the driver around a location. 
IotData | IoT data generator that creates a record with a device UUID, IP address, lat/long, date, and temperature.
PhoneNumber | Generates a phone number record. The area code can be specified in the processor configuration
TextMessage | Generates a text message records containing source phone number, source IMEI, destination phone number, timestamp, and a sample message.

All processors have an output record writer configuration and the number of output records.