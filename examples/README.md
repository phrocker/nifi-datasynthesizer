# Example schemas

Paste any of these into the **Record Schema** property of a `DataSynthesizer` processor, register them by name in
a `DataGenerator` controller service, or send them as FlowFile content to a `DataSynthesizer` with no schema set.

| File | What it produces |
|------|------------------|
| `person.json` | Name, SSN, email, street address and a real US zip with lat/long |
| `phone.json` | A phone number pinned to area code 212 |
| `iot.json` | A device with a 100 reading time series, one reading per minute, random-walk temperature |
| `web-log.json` | Long-tail distributed users, IP addresses, browsers and request timestamps |
| `text-message-llm.json` | SMS records whose body is written by the configured Text Generation Service |
| `support-ticket-llm.json` | Support tickets with an LLM-written description and a Markov fallback field |

The full list of samplers (`class` values) is in the upstream log-synth README:
https://github.com/tdunning/log-synth/blob/master/README.md. This bundle adds `ipv4`, `imei`, `double`, `vehicle`,
`toll`, `text`, `email`, `email-name`, `username`, `markov` and `llm`.
