import boto3
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
# TODO: Replace with your actual bucket names
PRIMARY_BUCKET = "my-primary-bucket"
SECONDARY_BUCKET = "my-secondary-bucket"

# --- STATE MANAGEMENT KEYS ---
# The main state file that tracks the last known good state of both buckets.
STATE_KEY = "replication_state.json"
# A temporary log file that tracks completed files for the CURRENT run.
# Its existence indicates that the previous run may have failed.
LOG_KEY = "replication.log.json"

# Initialize the S3 client
s3 = boto3.client("s3")


def now():
    """Return the current UTC time with timezone info in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def load_state():
    """
    Load the most recent replication state from either bucket.
    This provides redundancy if one bucket is temporarily down.
    """
    state = None
    # Check both buckets and use the newest state file
    for bucket in [PRIMARY_BUCKET, SECONDARY_BUCKET]:
        try:
            obj = s3.get_object(Bucket=bucket, Key=STATE_KEY)
            candidate = json.loads(obj["Body"].read().decode("utf-8"))
            if not state or candidate.get("last_updated", "") > state.get("last_updated", ""):
                state = candidate
        except ClientError:
            continue  # Silently ignore if the state file is not found in one bucket

    if state:
        return state

    # If no state file exists anywhere, start with a fresh state.
    print("No state file found. Starting fresh.")
    return {"files": {}, "last_updated": now()}


def save_state(state):
    """Save the final state to both buckets for redundancy."""
    state["last_updated"] = now()
    encoded = json.dumps(state, indent=2).encode("utf-8")

    for bucket in [PRIMARY_BUCKET, SECONDARY_BUCKET]:
        try:
            s3.put_object(Bucket=bucket, Key=STATE_KEY, Body=encoded)
        except ClientError as e:
            print(f"⚠️ Could not save final state to {bucket}: {e}")


def list_bucket_objects(bucket):
    """Return a dictionary {key: last_modified} for all objects in a bucket."""
    result = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                # Ignore the state and log files themselves
                if obj["Key"] not in [STATE_KEY, LOG_KEY]:
                    result[obj["Key"]] = obj["LastModified"].isoformat()
    except ClientError as e:
        print(f"❌ CRITICAL: Could not list objects in bucket {bucket}. Error: {e}")
    return result


def rebuild_files_state(primary_files, secondary_files):
    """Rebuilds the state['files'] dictionary from scratch after all sync actions are complete."""
    files = {}
    all_keys = set(primary_files) | set(secondary_files)

    for key in all_keys:
        locations = []
        if key in primary_files:
            locations.append("primary")
        if key in secondary_files:
            locations.append("secondary")
        
        # Determine the source based on modification time
        p_time = primary_files.get(key, "")
        s_time = secondary_files.get(key, "")
        source = "equal"
        if p_time > s_time:
            source = "primary"
        elif s_time > p_time:
            source = "secondary"

        files[key] = {
            "last_synced": now(),
            "last_seen_in": locations,
            "source": source,
        }
    return files


def load_progress_log():
    """Loads the list of keys successfully processed in a previous, failed run."""
    try:
        # The primary bucket is the source of truth for the log
        obj = s3.get_object(Bucket=PRIMARY_BUCKET, Key=LOG_KEY)
        return set(json.loads(obj["Body"].read().decode("utf-8")))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # This is the normal case: no log file means the last run was successful.
            return set()
        print(f"⚠️ Could not load progress log, proceeding without it: {e}")
        return set()


def save_progress_log(completed_keys):
    """Saves the list of completed keys to a temporary log file."""
    encoded = json.dumps(list(completed_keys), indent=2).encode("utf-8")
    for bucket in [PRIMARY_BUCKET, SECONDARY_BUCKET]:
        try:
            s3.put_object(Bucket=bucket, Key=LOG_KEY, Body=encoded)
        except ClientError as e:
            # This is not critical, but a warning is useful
            print(f"⚠️ Could not save progress log to {bucket}: {e}")


def delete_progress_log():
    """Removes the log file from both buckets after a successful run."""
    for bucket in [PRIMARY_BUCKET, SECONDARY_BUCKET]:
        try:
            s3.delete_object(Bucket=bucket, Key=LOG_KEY)
        except ClientError as e:
            print(f"⚠️ Could not delete progress log from {bucket}: {e}")


def replicate_resilient():
    """Performs a resilient, two-way sync of the S3 buckets."""
    state = load_state()
    
    completed_in_prior_run = load_progress_log()
    if completed_in_prior_run:
        print(f"✅ Resuming from a previous run. Skipping {len(completed_in_prior_run)} already completed files.")

    # This set will track keys successfully processed in THIS run
    completed_this_run = set(completed_in_prior_run)

    print("🔎 Discovering objects in both buckets...")
    primary_files = list_bucket_objects(PRIMARY_BUCKET)
    secondary_files = list_bucket_objects(SECONDARY_BUCKET)

    # Combine all keys: those currently in buckets and those from the last known state
    all_keys = set(primary_files) | set(secondary_files) | set(state.get("files", {}))
    
    print(f"Found {len(all_keys)} total unique objects to process.")

    for key in sorted(list(all_keys)):
        if key in completed_this_run:
            continue

        p_time = primary_files.get(key)
        s_time = secondary_files.get(key)
        in_state = key in state.get("files", {})
        
        action_taken = False
        
        try:
            # Exists in primary, missing in secondary
            if p_time and not s_time:
                if in_state and "secondary" in state["files"][key]["last_seen_in"]:
                    # The file existed in secondary before → it was deleted there
                    print(f"🗑️ {key}: Deleted from secondary → Removing from primary...")
                    s3.delete_object(Bucket=PRIMARY_BUCKET, Key=key)
                    action_taken = True
                else:
                    # New in primary → copy to secondary
                    print(f"📤 {key}: New in primary → Copying to secondary...")
                    s3.copy({"Bucket": PRIMARY_BUCKET, "Key": key}, SECONDARY_BUCKET, key)
                    action_taken = True

            # Exists in secondary, missing in primary
            elif s_time and not p_time:
                if in_state and "primary" in state["files"][key]["last_seen_in"]:
                    # The file existed in primary before → it was deleted there
                    print(f"🗑️ {key}: Deleted from primary → Removing from secondary...")
                    s3.delete_object(Bucket=SECONDARY_BUCKET, Key=key)
                    action_taken = True
                    
                else:
                    # New in secondary → copy to primary
                    print(f"📥 {key}: New in secondary → Copying to primary...")
                    s3.copy({"Bucket": SECONDARY_BUCKET, "Key": key}, PRIMARY_BUCKET, key)
                    action_taken = True

            # --- (Optional) Exists in both, check for updates ---
            # elif p_time and s_time:
            #     if p_time > s_time:
            #         print(f"🔄 {key}: Newer in primary → Updating secondary...")
            #         s3.copy({"Bucket": PRIMARY_BUCKET, "Key": key}, SECONDARY_BUCKET, key)
            #         action_taken = True
            #     elif s_time > p_time:
            #         print(f"🔄 {key}: Newer in secondary → Updating primary...")
            #         s3.copy({"Bucket": SECONDARY_BUCKET, "Key": key}, PRIMARY_BUCKET, key)
            #         action_taken = True

            
            # --- Deleted from both sides (known from state) ---
            # If a file was in the state but is now in neither p_time nor s_time,
            # no action is needed. The final state rebuild will correctly remove it.

            if action_taken:
                # --- CHECKPOINT ---
                completed_this_run.add(key)
                save_progress_log(completed_this_run)

        except ClientError as e:
            print(f"❌ ERROR processing {key}. It will be retried on the next run. Error: {e}")
            # Stop the script on error to prevent inconsistent state
            return

    print("\n✅ Sync actions complete. Rebuilding final state...")
    final_primary_files = list_bucket_objects(PRIMARY_BUCKET)
    final_secondary_files = list_bucket_objects(SECONDARY_BUCKET)
    state["files"] = rebuild_files_state(final_primary_files, final_secondary_files)
    save_state(state)

    print("🧹 Cleaning up progress log.")
    delete_progress_log()


if __name__ == "__main__":
    print(f"--- Starting S3 replication job at {now()} ---")
    replicate_resilient()
    print(f"--- Replication finished successfully at {now()} ---")