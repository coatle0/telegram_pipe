import argparse
from app.db import get_connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if args.apply:
        conn = get_connection(write=True)
    else:
        conn = get_connection()

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw_messages WHERE channel_id > 0")
    raw_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM processed_messages
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    processed_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM extracted_entities
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    ent_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM extracted_keywords
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    kw_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM defects
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    defect_count = cur.fetchone()[0]

    print("Fixture rows (channel_id > 0)")
    print("  raw_messages        =", raw_count)
    print("  processed_messages  =", processed_count)
    print("  extracted_entities  =", ent_count)
    print("  extracted_keywords  =", kw_count)
    print("  defects             =", defect_count)

    if not args.apply:
        print("Dry run only. Use --apply to delete these rows.")
        conn.close()
        return

    cur.execute(
        """
        DELETE FROM extracted_keywords
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    cur.execute(
        """
        DELETE FROM extracted_entities
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    cur.execute(
        """
        DELETE FROM defects
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    cur.execute(
        """
        DELETE FROM processed_messages
        WHERE raw_id IN (SELECT id FROM raw_messages WHERE channel_id > 0)
        """
    )
    cur.execute("DROP TRIGGER IF EXISTS prevent_raw_delete")
    cur.execute("DELETE FROM raw_messages WHERE channel_id > 0")
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS prevent_raw_delete
        BEFORE DELETE ON raw_messages
        BEGIN
            SELECT RAISE(ABORT, 'Delete not allowed on raw_messages');
        END;
        """
    )

    conn.commit()
    conn.close()
    print("Purge complete.")


if __name__ == "__main__":
    main()
