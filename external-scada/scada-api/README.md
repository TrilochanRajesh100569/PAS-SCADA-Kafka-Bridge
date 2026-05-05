# SCADA API Service (placeholder)

This folder is reserved for the SCADA API service that will:
- Subscribe to MQTT topic `tms/scada/pas` on the local RabbitMQ
- Decrypt AES-256-GCM payloads (if encryption enabled upstream)
- Translate RSAE JSON envelopes (CreatorId/Type/Timestamp/Alarm)
- Forward to ScateX / field HMIs

Uncomment the `scada-api:` service in `../docker-compose.yml` once the image is ready.

See repo `README.md` and `architecture-diagram-connect.html` for the full system architecture.

