# Changelog

## 2.9.0 (unreleased)

Revival for Apache NiFi 2.x. The 1.14.0 line targeted NiFi 1.14 and Java 11 and is no longer maintained.

### Platform
- Requires **Java 21** and **Apache NiFi 2.9.0** or later. Versions now track the NiFi release they are built against.
- Standalone Maven build with `nifi-nar-maven-plugin` 2.3.0; the retired `nifi-nar-bundles` parent is gone.
- Continuous integration runs on JDK 21 and uploads the three NARs as workflow artifacts.

### New
- **Chaos engineering**: a `chaos` schema sampler that corrupts a configurable fraction of any field (null,
  empty, wrong type, truncation, oversize, unicode noise, whitespace, benign injection canaries, outliers), and a
  per-processor chaos profile (duplicate, drop and drift rates, seed). With Chaos Enabled, records carry a
  `chaos_id` and a manifest of every injected fault is routed to the new `chaos` relationship so experiments can
  assert exactly what the pipeline should have caught.
- **LLM-backed text**: `llm` sampler for any log-synth schema field, with per-field prompt, system prompt, token
  limit and batching (one API call yields many values). Two controller services implement the new
  `TextGenerationService` API: `AnthropicTextGenerationService` (official Anthropic Java SDK, refusal fallbacks on
  by default) and `OpenAITextGenerationService` (official OpenAI Java SDK, works against any OpenAI-compatible
  endpoint such as Ollama or vLLM). Every processor has an optional **Text Generation Service** property.
- **`markov` sampler**: offline Markov-chain prose from a bundled corpus (`alice_oz`, `us_const`) or a text file.
  `TextGenerator` and `TextMessage` use it when no LLM service is configured, and each record now gets its own text.
- `DataGenerator` controller service validates schemas at configuration time and accepts a Text Generation Service
  so shared schemas can use `llm` fields.
- `DataSynthesizer` routes consumed schema FlowFiles to a new `original` relationship and failures to `failure`.
- `CommuterData` gained a **Work Distance** property.
- Output FlowFiles carry `record.count` and `mime.type` attributes.

### Changed
- Records are built directly from sampler output instead of being serialized to JSON and parsed back. This removes
  the dependency on the record serialization services bundle and the old `org.codehaus.jackson` classes.
- `DataCorrelator`: **Record Count** is now **Correlations Per Record** and is exact (it was a random 0 to 4 before).
  Generated pieces are batched into one `correlation` FlowFile per input instead of one FlowFile per piece.
- `Transactions`: transaction counts are now `Max Transactions` when fixed, otherwise uniform between 0 and the
  maximum with **Allow Zero Transactions** honoured literally. The previous arithmetic produced zero transactions
  with the default settings.
- `java-faker` (unmaintained since 2020) replaced by **Datafaker** 2.4.
- log-synth dependencies refreshed (Guava 33, Jackson 2.21, Mahout Math 0.13, FreeMarker 2.3.34); the log-synth
  CLI, log generator and trajectory visualizer were dropped along with their args4j, Processing, FastPFOR and
  XML dependencies. Sampler resource files moved into the generators jar.
- Tests migrated to JUnit 5.

### Known
- Jackson core jars ship in all three NARs: the NAR plugin bundles them when SDK jars pull them transitively even
  though they resolve as provided. Versions are pinned by the Jackson BOM and parent-first delegation uses the
  service API NAR's copy, so this is dead weight (about 2.4 MB per NAR), not a conflict.

### Removed
- `RemoveRecordPath` processor (an Accumulo-era utility; use `UpdateRecord`).
- The service API no longer depends on the generators jar; `LookupSampler` moved to the processors module.
