-- Iris universal notify function
-- Install once, reuse across any table.
-- Channel name is passed as a trigger argument — no hardcoding required.
-- Cycle guard: only fires if processed_at IS NULL (row not yet handled).

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


-- Example: attach to any table with any channel name
--
-- CREATE TRIGGER orders_iris_trigger
--     AFTER INSERT OR UPDATE ON orders
--     FOR EACH ROW
--     EXECUTE FUNCTION iris_notify('order_status_changed');
--
-- CREATE TRIGGER inventory_iris_trigger
--     AFTER INSERT OR UPDATE ON inventory
--     FOR EACH ROW
--     EXECUTE FUNCTION iris_notify('inventory_updated');
