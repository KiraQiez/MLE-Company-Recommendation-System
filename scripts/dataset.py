"""Inspect the offline registry feed before implementing the queue consumer."""

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DATASET = Path(__file__).resolve().parents[1] / "data" / "feed.jsonl"

COMPANY_FIELDS = (
    "companyName",
    "natureOfBusiness",
    "state",
    "industry",
)


def parse_timestamp(value: object) -> datetime | None:
    """Return the source time in UTC, or None if it cannot be ordered reliably."""
    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def inspect_dataset(path: Path = DATASET) -> None:
    """Report feed quality and ordering without changing any records."""
    total_lines = 0
    invalid_json = []
    non_object = []
    event_types = Counter()
    missing_fields = Counter()
    malformed_events = Counter()
    registration_counts = Counter()
    seen_bodies: set[str] = set()
    latest_timestamps: dict[str, datetime] = {}
    duplicate_bodies = 0
    out_of_order = 0

    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            total_lines += 1

            # The producer hashes the body without its line ending for AMQP message_id.
            body = line.removesuffix("\n")
            if body in seen_bodies:
                duplicate_bodies += 1
            seen_bodies.add(body)

            try:
                event = json.loads(body)
            except json.JSONDecodeError:
                invalid_json.append(line_number)
                continue

            if not isinstance(event, dict):
                non_object.append(line_number)
                continue

            event_type = event.get("type")
            event_types[str(event_type)] += 1

            if event_type not in ("company-upserted", "company-struck-off"):
                malformed_events["unknown_event_type"] += 1

            registration = event.get("registrationNumber")

            if not isinstance(registration, str) or not registration.strip():
                malformed_events["missing_registration_number"] += 1
                registration = None
            else:
                registration_counts[registration] += 1

            timestamp = parse_timestamp(event.get("updatedAt"))

            if timestamp is None:
                malformed_events["invalid_timestamp"] += 1
            elif registration is not None:
                # Compare with the newest source time seen for this company so far.
                latest = latest_timestamps.get(registration)
                if latest is None or timestamp > latest:
                    latest_timestamps[registration] = timestamp
                elif timestamp < latest:
                    out_of_order += 1

            # Inspect card fields even when the event has other data quality issues.
            if event_type == "company-upserted":
                data = event.get("data")

                if not isinstance(data, dict):
                    malformed_events["invalid_upsert_data"] += 1
                    continue

                for field in COMPANY_FIELDS:
                    value = data.get(field)

                    if not isinstance(value, str) or not value.strip():
                        missing_fields[field] += 1

    repeated_companies = sum(
        count > 1 for count in registration_counts.values()
    )

    print("=" * 55)
    print("ONECREDIT DATASET INSPECTION")
    print("=" * 55)

    print(f"Total lines:                 {total_lines}")
    print(f"Invalid JSON lines:          {len(invalid_json)}")
    print(f"Non-object JSON values:      {len(non_object)}")
    print(f"JSON objects:                {sum(event_types.values())}")
    print(f"Exact duplicate bodies:      {duplicate_bodies}")
    print(f"Unique registration numbers: {len(registration_counts)}")
    print(f"Repeated companies:          {repeated_companies}")
    print(f"Older than seen time:         {out_of_order}")

    print("\nEVENT TYPES")
    for event_type, count in event_types.most_common():
        print(f"  {event_type}: {count}")

    print("\nMISSING / INVALID FIELDS IN UPSERT DATA OBJECTS")
    for field in COMPANY_FIELDS:
        print(f"  {field}: {missing_fields[field]}")

    print("\nMALFORMED EVENT FIELDS")
    for reason, count in malformed_events.most_common():
        print(f"  {reason}: {count}")

    print("\nINVALID JSON LINE NUMBERS")
    print(invalid_json[:20])

    print("\nNON-OBJECT JSON LINE NUMBERS")
    print(non_object[:20])

    print("\nMOST FREQUENT REGISTRATION NUMBERS")
    for registration, count in registration_counts.most_common(5):
        print(f"  {registration}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("feed", type=Path, nargs="?", default=DATASET)
    args = parser.parse_args()

    try:
        inspect_dataset(args.feed)
    except (OSError, UnicodeError) as error:
        parser.exit(1, f"Could not read {args.feed}: {error}\n")


if __name__ == "__main__":
    main()
