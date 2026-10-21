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

[![Maven Build](https://github.com/phrocker/nifi-datasynthesizer/actions/workflows/maven.yml/badge.svg)](https://github.com/phrocker/nifi-datasynthesizer/actions/workflows/maven.yml)

Schema-driven synthetic data for **Apache NiFi 2.x**. Describe a record with a small JSON schema and get records
whose fields are statistically related: long-tail user populations, real US geography, time series with
event rates and random walks, commuter trajectories, fraud-style transaction pools, and free text written either
offline by a Markov chain or by a large language model.

The sampling engine is [log-synth](https://github.com/tdunning/log-synth) by Ted Dunning, extended with
[Datafaker](https://www.datafaker.net/) samplers and LLM text generation through the official
[Anthropic](https://github.com/anthropics/anthropic-sdk-java) and [OpenAI](https://github.com/openai/openai-java)
Java SDKs.

## How this differs from NiFi's GenerateRecord

NiFi ships `GenerateRecord`, which fills each field of an Avro schema independently with Datafaker values. It is
the right tool for "give me rows with a name, an email and a number". This bundle is for data whose fields have to
hang together: a zip code whose city, state and coordinates agree, a device whose hundred readings are a minute
apart and drift like a real sensor, a pool of merchant terminals shared by every transaction, a commuter who
drives from a real home to a real workplace, or a message body that reads like a person wrote it.

## Requirements

- Apache NiFi 2.9.0 or later
- Java 21 to build

## Build and install

```bash
mvn -B clean install
cp nifi-datasynthesizer-service-api-nar/target/*.nar \
   nifi-datasynthesizer-service-nar/target/*.nar \
   nifi-datasynthesizer-nar/target/*.nar  $NIFI_HOME/lib/
```

Restart NiFi. All three NARs are required; the processors NAR and the services NAR both depend on the service API NAR.

## Processors

| Processor | Description |
|-----------|-------------|
| `DataSynthesizer` | Generates records from any log-synth schema. The schema can be set on the processor, looked up by name from a `DataGenerator` service, or taken from the content of each incoming FlowFile. |
| `DataCorrelator` | Enriches each incoming record with generated data under a chosen key and emits the generated pieces separately on `correlation`, so the two streams can be joined downstream. |
| `CommuterData` | Simulates a commuter's daily drive: home (optionally pinned to a zip code), workplace, and a GPS trajectory with vehicle telemetry. |
| `IotData` | Device UUID, IP, real continental-US coordinates and a 100 reading time series with a random-walk temperature. |
| `Transactions` | Credit card transactions against a shared pool of merchant terminals with real addresses. With input connected, every incoming record gains a `transactions` array. |
| `PhoneNumber` | Phone number records, optionally pinned to an area code. |
| `TextMessage` | SMS records: source IMEI, source and destination numbers, timestamp and a message body. |
| `TextGenerator` | Records containing a block of free text. |

Every processor takes a **Record Writer** (any NiFi record writer: JSON, Avro, CSV, Parquet...) and a
**Record Count** per FlowFile, and writes `record.count` and `mime.type` attributes.

`TextMessage`, `TextGenerator`, `DataSynthesizer` and `DataCorrelator` also take an optional
**Text Generation Service**. When set, text fields are written by the model; when unset, `TextMessage` and
`TextGenerator` fall back to the offline Markov chain.

## Controller services

| Service | Description |
|---------|-------------|
| `DataGenerator` | A catalog of named schemas. Each dynamic property is a schema (inline JSON or a file path). Processors reference the name, so one definition serves many processors. |
| `AnthropicTextGenerationService` | Text generation with Claude. Properties: API Key, Model (default `claude-opus-5-5`; `claude-haiku-5-5` is the budget choice), Effort (default `low`), Refusal Fallbacks (default on), timeout, retries, optional Base URL. |
| `OpenAITextGenerationService` | Text generation through the Chat Completions API. Point **Base URL** at `http://localhost:11434/v1` for Ollama, or at vLLM, LM Studio or any other OpenAI-compatible server, to generate with local models. |

Both text services implement `TextGenerationService`, so a third provider is one class away.

## Schemas

A schema is a JSON array of samplers. Each sampler has a `name` and a `class`, plus class-specific options.

```json
[
  {"name": "id", "class": "uuid"},
  {"name": "user", "class": "pitman_yor", "alpha": 10, "discount": 0.5, "base": {"class": "username"}},
  {"name": "where", "class": "zip", "fields": "city, state, latitude, longitude"},
  {"name": "when", "class": "event", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "rate": "10/s"},
  {"name": "note", "class": "llm", "prompt": "Write a one-line delivery note.", "maxTokens": 40, "batch": 25}
]
```

The upstream [log-synth README](https://github.com/tdunning/log-synth/blob/master/README.md) documents the core
samplers (`int`, `string`, `name`, `address`, `zip`, `date`, `event`, `random-walk`, `sequence`, `map`, `join`,
`flatten`, `foreign-key`, `common-point-of-compromise`, `commuter`, `vin`, `browser`, `os`, `ssn`, ...).
This bundle adds:

| class | Options | Produces |
|-------|---------|----------|
| `llm` | `prompt` (required), `system`, `maxTokens` (128), `batch` (1), `temperature` | Text from the configured Text Generation Service. With `batch` > 1 one API call yields that many values. |
| `markov` | `corpus` (`alice_oz`, `us_const`, or a file path), `length` (128 words), `order` (3) | Offline Markov-chain prose |
| `ipv4` | `private` | IPv4 address |
| `imei` | | 15 digit IMEI |
| `email`, `email-name`, `username` | | Email addresses and handles |
| `text` | `type` (`chucknorris`, `backtothefuture`, `gameofthrones`) or `text` strings | Short quotes |
| `double` | `min`, `max` | Floating point values |
| `vehicle`, `toll` | | Vehicle certification and toll records |

More examples live in [`examples/`](examples/).

### Cost control for `llm` fields

Each `llm` sampler batches: with `"batch": 20` it asks the model for twenty distinct variants in a single request
and hands them out one per record, so a thousand records cost fifty calls rather than a thousand. Keep `maxTokens`
small for short fields, use `low` effort and a Haiku-class or local model for volume, and prefer one `llm` field per
schema. The sampler records the prompt it sent, so a flow can be tested against the mock service in the test suite
without touching an API.

## Example flows

**Card holders with purchases**: `DataSynthesizer` with `examples/person.json` → `Transactions` (Record Reader set,
Max Transactions 5) → `PutDatabaseRecord`. Each person record arrives with a `transactions` array against the shared
terminal pool.

**Chat traffic for an NLP pipeline**: `TextMessage` with an `OpenAITextGenerationService` pointed at a local Ollama
model, LLM Batch Size 20, Record Count 200 → `PublishKafkaRecord`.

**Sensor fleet**: `IotData`, Record Count 50 → `ConvertRecord` to Parquet → `PutS3Object`.

## Development

```bash
mvn -B verify                                    # build, run all tests, package NARs
mvn -B -pl nifi-datasynthesizer-processors test  # one module
```

Modules:

| Module | Contents |
|--------|----------|
| `nifi-datasynthesizer-generators` | log-synth engine (vendored, package `com.mapr.synth`), Datafaker samplers, `markov` and `llm` samplers, sampler resource files. No NiFi dependency. |
| `nifi-datasynthesizer-service-api` | `SchemaLookupService`, `TextGenerationService` |
| `nifi-datasynthesizer-service` | `DataGenerator`, `AnthropicTextGenerationService`, `OpenAITextGenerationService` |
| `nifi-datasynthesizer-processors` | The processors and the JSON-to-record converter |
| `*-nar` | NAR packaging |

The LLM provider tests run against a local HTTP stub and assert on the wire format; nothing in the build calls a
real API.

See [CHANGELOG.md](CHANGELOG.md) for what changed from the 1.x line.
