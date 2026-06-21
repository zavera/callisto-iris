# Callisto Iris

**Postgres-native event orchestration. Zero brokers.**

Iris turns PostgreSQL `LISTEN`/`NOTIFY` into a fully managed event pipeline — with typed routing, auto-reconnect, retry logic, and dead-letter handling — without Kafka, RabbitMQ, or any external message broker.

---

## Why Iris

If you're already on Postgres, you don't need a separate broker for internal event-driven workflows. Postgres has had `LISTEN`/`NOTIFY` since 1998. Iris gives it the orchestration layer it was always missing.

- **Zero new infrastructure** — if you have Postgres, you have the whole stack
- **Auto-reconnect** — survives Postgres restarts and host reboots; re-registers all channels automatically
- **Typed routing** — each channel routes to its own handler
- **Retry + dead-letter** — failed handlers retry with backoff; unrecoverable events land in a dead-letter channel
- **DB-agnostic design** — transport is a pluggable interface; Postgres is the first implementation

---

## Quickstart

### 1. Install the trigger function (once per database)

```sql
-- sql/iris_notify.sql
CREATE OR REPLACE FUNCTION iris_notify()
RETURNS trigger AS $$
BEGIN
    IF NEW.processed_at IS NULL THEN
        PERFORM pg_notify(
            TG_ARGV[0],
            row_to_json(NEW)::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### 2. Attach to any table

```sql
CREATE TRIGGER orders_iris_trigger
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION iris_notify('order_status_changed');
```

The trigger only fires when `processed_at IS NULL` — preventing cyclic re-triggering when your handler writes back to the same row.

### 3. Wire up the pipeline in Java

```java
PgTransportConfig config = PgTransportConfig.builder()
    .host("localhost")
    .database("mydb")
    .username("postgres")
    .password("secret")
    .initialBackoffMs(1000)
    .maxBackoffMs(30000)
    .build();

IrisPipeline pipeline = IrisPipeline.builder(new PgTransport(config))
    .listen("order_status_changed", (channel, payload) -> {
        // parse payload, call your service, write back processed_at
    })
    .listen("inventory_updated", (channel, payload) -> {
        // handle inventory events
    })
    .maxRetries(3)
    .build();

pipeline.start();
```

---

## Cycle Guard

The `iris_notify()` function uses `processed_at IS NULL` as a sentinel to prevent infinite trigger loops:

| `processed_at` | Meaning |
|---|---|
| `NULL` | Pending — trigger will fire |
| timestamp | Processed — trigger will not re-fire |

Your handler sets `processed_at = NOW()` on write-back. If the handler crashes before writing back, the row stays `NULL` and will be retried on the next event or a scheduled sweep.

---

## Auto-Reconnect

Iris automatically detects connection drops via pgjdbc-ng's `PGNotificationListener.closed()` callback and reconnects with exponential backoff. All `LISTEN` subscriptions are re-registered on the new connection — no manual intervention required.

```
Connection drops (Postgres restart / host reboot)
    → closed() fires
    → exponential backoff reconnect loop
    → new connection established
    → all channels re-registered automatically
```

---

## Transport Interface

Iris is DB-agnostic by design. The transport layer is a pluggable interface:

```java
public interface IrisTransport {
    void connect();
    void subscribe(String channel, TransportListener listener);
    void unsubscribe(String channel);
    void disconnect();
    boolean isConnected();
}
```

`PgTransport` is the Postgres implementation. Additional transports can be added without changing routing, cycle guard, or handler logic.

---

## Built by

[Callisto Tech](https://callistotech.io) — tools for event-driven data infrastructure.
