-- P4: LISTEN/NOTIFY trigger for outbox dispatcher.
-- Run once against orderflow_ops:
--   psql -U postgres -d orderflow_ops -f sql/migrations/outbox_notify_trigger.sql
--
-- Fires pg_notify('outbox_ready', exchange_name) on every INSERT into
-- ops.outbox_event (including all child partitions, PG13+).
-- The Rust outbox dispatcher wakes immediately instead of polling every 50ms.

CREATE OR REPLACE FUNCTION ops.notify_outbox_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox_ready', NEW.exchange_name);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_outbox_notify ON ops.outbox_event;
CREATE TRIGGER trg_outbox_notify
AFTER INSERT ON ops.outbox_event
FOR EACH ROW EXECUTE FUNCTION ops.notify_outbox_ready();
