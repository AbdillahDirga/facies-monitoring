import argparse
import json
import os
import threading
import time

import pandas as pd
import paho.mqtt.client as mqtt

CHECKPOINT_DIR = "checkpoints"
WELL_COLUMN = "Well Name"
DEPTH_COLUMN = "Depth"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay well log data over MQTT")
    parser.add_argument(
        "--data",
        type=str,
        default="../data/raw/facies_vectors.csv",
        help="Path to the facies CSV file (default: ../data/raw/facies_vectors.csv)",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default="localhost",
        help="MQTT broker host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=1883,
        help="MQTT broker port (default: 1883)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds to wait between rows, per well (default: 1.0)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear all saved checkpoints and replay every well from the start",
    )
    return parser.parse_args()


def checkpoint_path(well_name: str) -> str:
    # Well names can contain spaces (e.g. "CROSS H CATTLE"), so they are
    # normalized into a filesystem-safe filename.
    safe_name = well_name.replace(" ", "_")
    return os.path.join(CHECKPOINT_DIR, f"{safe_name}.checkpoint")


def load_checkpoint(well_name: str) -> int:
    # Returns the index of the next row to send for this well.
    # If no checkpoint exists yet, the well starts from row 0.
    path = checkpoint_path(well_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return int(f.read().strip())
    return 0


def save_checkpoint(well_name: str, row_index: int) -> None:
    path = checkpoint_path(well_name)
    with open(path, "w") as f:
        f.write(str(row_index))


def reset_checkpoints() -> None:
    if not os.path.exists(CHECKPOINT_DIR):
        return
    for filename in os.listdir(CHECKPOINT_DIR):
        os.remove(os.path.join(CHECKPOINT_DIR, filename))
    print("All checkpoints cleared.\n")


def row_to_payload(row: pd.Series) -> str:
    # Converts a dataset row into a JSON string for MQTT.
    # NaN values (e.g. missing PE readings) become JSON null automatically.
    record = row.where(pd.notnull(row), None).to_dict()
    return json.dumps(record)


def replay_well(client: mqtt.Client, well_name: str, well_df: pd.DataFrame, interval: float) -> None:
    # Runs in its own thread. Publishes one row at a time, in depth order,
    # to this well's dedicated topic, saving a checkpoint after every send.
    topic = f"facies/welllog/{well_name}"
    start_index = load_checkpoint(well_name)
    total_rows = len(well_df)

    if start_index >= total_rows:
        print(f"[{well_name}] Already complete ({total_rows} rows). Skipping.")
        return

    print(f"[{well_name}] Starting at row {start_index}/{total_rows} -> topic '{topic}'")

    for i in range(start_index, total_rows):
        row = well_df.iloc[i]
        payload = row_to_payload(row)
        result = client.publish(topic, payload)
        result.wait_for_publish(timeout=5)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[{well_name}] WARNING: publish failed for row {i} (rc={result.rc})")
        save_checkpoint(well_name, i + 1)
        time.sleep(interval)

    print(f"[{well_name}] Finished all {total_rows} rows.")


def main():
    args = parse_args()

    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    if args.reset:
        reset_checkpoints()

    df = pd.read_csv(args.data)
    df = df.sort_values(by=DEPTH_COLUMN)  # ensure ascending depth order within each well

    # Compatible with both paho-mqtt v1.x and v2.x, which changed the
    # Client() constructor signature.
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    else:
        client = mqtt.Client()

    def on_connect(client, userdata, flags, rc, *extra):
        if rc == 0:
            print(f"Connected to MQTT broker at {args.broker}:{args.port}\n")
        else:
            print(f"FAILED to connect to broker (rc={rc}). Check broker address/port.\n")

    def on_disconnect(client, userdata, rc, *extra):
        print(f"Disconnected from broker (rc={rc})")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    wells = df[WELL_COLUMN].unique()
    print(f"Found {len(wells)} wells. Launching one thread per well.\n")

    threads = []
    for well_name in wells:
        well_df = df[df[WELL_COLUMN] == well_name].reset_index(drop=True)
        t = threading.Thread(target=replay_well, args=(client, well_name, well_df, args.interval))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    client.loop_stop()
    client.disconnect()
    print("\nAll wells finished replaying.")


if __name__ == "__main__":
    main()