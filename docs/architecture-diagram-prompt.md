# Architecture Diagram — Image-Gen Prompt

Source-of-truth prompt for regenerating the master architecture image
(TMS ↔ SCADA · Kafka Connect Pipeline · with DLQ + Camel-legacy + Streams).

Use with: ChatGPT image / DALL·E 3 / GPT-Image-1.
Output: 16:10 landscape PNG, saved as `docs/architecture.png`.

---

## Prompt

```
A landscape 16:10 cloud architecture infographic poster, in the style of
official Confluent and AWS Architecture Center documentation diagrams.
Flat 2D vector design, white background, soft pastel zone backgrounds,
crisp 1.5pt borders, minimalist line icons, generous whitespace,
professional and breathable. Bold Inter / SF Pro sans-serif typography.
No 3D, no glow, no gradients beyond zone tinting.

TOP — full-width title bar:
"TMS ↔ SCADA Integration — Kafka Connect Pipeline"
Below it, smaller grey subtitle:
"Outbound (TMS → SCADA) · Inbound (SCADA → TMS) · Verified Live"

Two large horizontal arrows below the title:
purple right-arrow on the left half labeled "OUTBOUND",
orange left-arrow on the right half labeled "INBOUND".

MIDDLE — six zone columns, evenly spaced, color-coded:

1. Indigo zone "TMS / ARTEMIS" with a building icon.
   Cards: "Artemis Broker", "TMS.PISInfo", "SCADA.TMS.Alarms".

2. Cyan zone "KAFKA CONNECT" with a plug icon.
   Four connector cards stacked, each with a small green RUNNING dot:
   "tms-artemis-source", "tms-rabbitmq-sink",
   "scada-rabbitmq-source", "scada-artemis-sink".
   Below them, a red dashed-border card titled "DLQ × 4"
   listing "dlq.connect.*".

3. Green zone "APACHE KAFKA" with a database cylinder icon.
   Topic cards top-to-bottom: "tms.raw", amber transform box
   "Streams: XML→JSON + AES-256", "tms.scada.encrypted",
   thin divider, "scada.tms.raw", amber box
   "Streams: Decrypt + Validate", "scada.tms.processed".

4. Purple zone "APACHE CAMEL (legacy)" with a rocket icon, narrower.
   Two small cards: "Camel-1 outbound", "Camel-2 inbound".

5. Orange zone "RABBITMQ + MQTT" with a hub icon.
   Cards: "Exchange amq.topic", "Queue scada.tms.alarms",
   "MQTT TLS 8883", "Mgmt UI 15672".

6. Blue zone "SCADA SYSTEM" with an antenna/broadcast icon.
   Two cards: "SCADA Consumer (MQTT sub)",
   "SCADA Producer (MQTT pub)".

Connect zones with thin solid arrows:
purple arrows flowing left-to-right across the top half (outbound),
orange arrows flowing right-to-left across the bottom half (inbound),
short red dashed arrows from each connector card down to the DLQ block.

BELOW the main grid — a full-width horizontal strip card titled
"END-TO-END FLOWS" containing two rows of small labeled icons:

OUTBOUND row (purple chevrons between):
Artemis → Connect-Source → Kafka topic → Streams → Kafka topic
→ Connect-Sink → RabbitMQ → MQTT → SCADA Consumer.

INBOUND row (orange chevrons between):
SCADA Producer → MQTT → RabbitMQ → Connect-Source → Kafka topic
→ Streams → Kafka topic → Connect-Sink → Artemis.

BOTTOM — five small info cards in a single row:

1. "COMPONENTS" with five short bullets (Artemis, Kafka,
   Kafka Connect, Streams, RabbitMQ).
2. "RELIABILITY & DLQ" — bullets: idempotent producer,
   acks=all, errors.tolerance=all, 4 DLQ topics, 7d retention.
3. "MONITORING" — bullets: Connect REST status, Kafka lag,
   MQTT health, alerts via email/Slack.
4. "PORTS" — bullets: Artemis 61616, Kafka 9092, Connect 8083,
   RMQ 5672, MQTT 8883.
5. "SECURITY" with padlock icons — AES-256-GCM, RSA validation,
   MQTT TLS, k8s Secrets, least privilege.

Color palette (use exactly):
white #ffffff background, ink #1e293b text, indigo #6366f1,
cyan #0891b2, green #16a34a, amber #ca8a04, purple #7c3aed,
orange #ea580c, blue #2563eb, red #dc2626 (dashed for DLQ).
Zone backgrounds at 10% opacity of their accent color.

Render all text crisply, prioritize legibility over decoration,
no fine print smaller than 8pt equivalent. Looks like a real,
published cloud architecture documentation poster.
```

---

## Refinement #1 — Camel placement fix

After the first generation, paste this follow-up to move Camel
into the correct architectural position:

```
Same diagram, one structural fix:
Move the "Apache Camel (legacy)" column to OVERLAP the Apache Kafka
column — show Camel-1 outbound as a small purple box sitting between
"tms.raw" and "tms.scada.encrypted" (alongside the amber Streams
transform), and Camel-2 inbound as a purple box between
"scada.tms.raw" and "scada.tms.processed" (alongside the amber
Streams: Decrypt+Validate). Add a small "OR" label between each
Streams box and the corresponding Camel box to show they are
interchangeable transform options. Keep everything else identical.
```

## Refinement #2 — direction tags + footer

Final polish — adds direction arrows on connectors, pub/sub tags on
TMS topics, and a context footer:

```
Same diagram, three small refinements only — keep everything else
identical:

1. On each of the 4 Kafka Connect connector cards, add a tiny
   directional arrow before the name: a small purple → arrow before
   "tms-artemis-source" and "tms-rabbitmq-sink" (outbound), and a
   small orange ← arrow before "scada-rabbitmq-source" and
   "scada-artemis-sink" (inbound).

2. In the TMS / Artemis column, add small tags on the topic cards:
   "TMS.PISInfo" gets a tiny purple "OUT ▶" pill in the corner,
   "SCADA.TMS.Alarms" gets a tiny orange "◀ IN" pill.

3. Add a small footer line below the bottom info-card row, centered,
   thin grey text:
   "v1 · 2026-04-29 · runs on Kubernetes (minikube / pinkline namespace)
    · all 4 connectors RUNNING · verified end-to-end".
```
