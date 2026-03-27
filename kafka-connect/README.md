# Kafka Connect -- Iceberg Sink (DEPRECATED)

> **This component has been replaced by the PyIceberg writer (`pyiceberg-sidecar/iceberg_writer.py`).**
>
> The Kafka Connect Iceberg Sink runtime ZIP is not available as a pre-built
> download and must be compiled from source. PyIceberg was the documented
> "valid fallback" path in the V3 architecture document and provides superior
> functionality (native upsert with identifier fields, simpler deployment).

---

## Replacement

| Original Plan | Replacement |
|--------------|-------------|
| Kafka Connect Iceberg Sink (append) | `pyiceberg-sidecar/iceberg_writer.py` (append via PyIceberg) |
| PyIceberg sidecar (upsert only) | `pyiceberg-sidecar/finality_updater.py` (upsert via PyIceberg) |

See `pyiceberg-sidecar/README.md` for full documentation.
