import requests
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('snapshot_restore.log'),
        logging.StreamHandler()
    ]
)

# Cluster configurations
ACTIVE_CLUSTER = {
    'url': 'http://localhost:9203',  # Active cluster URL
}

PASSIVE_CLUSTER = {
    'url': 'http://localhost:9201',  # Passive cluster URL
}

# Shared snapshot repository configuration
SNAPSHOT_REPO = 'snapshot-repo-2'  # Your snapshot repository name
MAX_RESTORE_BYTES = 500 * 10**9

# System indices that should be skipped
SYSTEM_INDICES = [
    '.kibana',
    '.kibana_',
    '.security',
    '.apm',
    '.async-search',
    '.ds-',
    '.internal.',
    '.slo-'
]

def is_system_index(index_name):
    """Check if an index is a system index."""
    return any(index_name.startswith(prefix) for prefix in SYSTEM_INDICES)

def verify_repository(cluster_config, repo_name):
    """Verify if the repository exists on the cluster."""
    try:
        # First check if repository exists
        r = requests.get(f"{cluster_config['url']}/_snapshot/{repo_name}")
        r.raise_for_status()
        
        # Then verify repository status
        status_r = requests.get(f"{cluster_config['url']}/_snapshot/{repo_name}/_status")
        status_r.raise_for_status()
        
        logging.info(f"Repository {repo_name} exists and is accessible on {cluster_config['url']}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Repository {repo_name} verification failed on {cluster_config['url']}: {str(e)}")
        if hasattr(e.response, 'text'):
            logging.error(f"Error details: {e.response.text}")
        return False

def get_latest_snapshot():
    """Get the latest snapshot from the active cluster."""
    try:
        r = requests.get(f"{ACTIVE_CLUSTER['url']}/_cat/snapshots/{SNAPSHOT_REPO}?format=json")
        r.raise_for_status()
        snapshots = r.json()
        if not snapshots:
            logging.error(f"No snapshots found in repository {SNAPSHOT_REPO}")
            return None
        latest = max(snapshots, key=lambda s: s['end_epoch'])
        logging.info(f"Found latest snapshot: {latest['id']} from {datetime.fromtimestamp(int(latest['end_epoch'])/1000)}")
        return latest['id']
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get latest snapshot from {ACTIVE_CLUSTER['url']}: {str(e)}")
        return None

def verify_snapshot_exists(snapshot):
    """Verify if the snapshot exists in the repository on passive cluster."""
    try:
        r = requests.get(f"{PASSIVE_CLUSTER['url']}/_snapshot/{SNAPSHOT_REPO}/{snapshot}")
        r.raise_for_status()
        logging.info(f"Snapshot {snapshot} exists in repository {SNAPSHOT_REPO}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Snapshot {snapshot} not found in repository {SNAPSHOT_REPO}: {str(e)}")
        return False

def get_snapshot_indices(snapshot):
    """Get indices and their sizes from a snapshot."""
    try:
        r = requests.get(f"{ACTIVE_CLUSTER['url']}/_snapshot/{SNAPSHOT_REPO}/{snapshot}/_status")
        r.raise_for_status()
        indices = r.json()['snapshots'][0]['indices']
        return {name: idx['stats']['total']['size_in_bytes'] for name, idx in indices.items()}
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get snapshot indices from {ACTIVE_CLUSTER['url']}: {str(e)}")
        return {}

def get_local_indices():
    """Get indices and their stats from the passive cluster."""
    try:
        r = requests.get(f"{PASSIVE_CLUSTER['url']}/_cat/indices?format=json&h=index,docs.count,store.size")
        r.raise_for_status()
        indices = {}
        for idx in r.json():
            size = idx.get('store.size', '0b')  # Default to 0b if size is None
            docs_count = idx.get('docs.count', '0')  # Default to '0' if docs.count is None
            
            # Convert size to bytes
            size_bytes = 0
            if size and isinstance(size, str):
                try:
                    if size.endswith('b'):
                        num = float(size[:-2])
                        if size.endswith('kb'): num *= 1024
                        elif size.endswith('mb'): num *= 1024**2
                        elif size.endswith('gb'): num *= 1024**3
                        elif size.endswith('tb'): num *= 1024**4
                        size_bytes = int(num)
                    else:
                        size_bytes = int(size)
                except (ValueError, TypeError) as e:
                    logging.warning(f"Could not parse size '{size}' for index {idx['index']}: {str(e)}")
                    size_bytes = 0
            
            # Convert docs count to integer
            try:
                docs = int(docs_count) if docs_count is not None else 0
            except (ValueError, TypeError) as e:
                logging.warning(f"Could not parse docs count '{docs_count}' for index {idx['index']}: {str(e)}")
                docs = 0
            
            indices[idx['index']] = {
                'docs': docs,
                'size': size_bytes
            }
            logging.debug(f"Index {idx['index']}: size={size_bytes} bytes, docs={docs}")
        
        return indices
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get local indices from {PASSIVE_CLUSTER['url']}: {str(e)}")
        return {}

def pick_indices_to_restore(snapshot_indices, local_indices):
    """Select indices to restore based on size differences."""
    to_restore = []
    for idx, snap_size in snapshot_indices.items():
        # Skip system indices
        if is_system_index(idx):
            logging.info(f"Skipping system index: {idx}")
            continue
            
        local = local_indices.get(idx)
        if not local or snap_size > local['size']:
            to_restore.append((idx, snap_size))
            logging.info(f"Index {idx} needs restore: snapshot size {snap_size}, local size {local['size'] if local else 0}")
    
    # Sort by size (largest first)
    to_restore.sort(key=lambda x: -x[1])
    batch, total = [], 0
    for idx, size in to_restore:
        if total + size > MAX_RESTORE_BYTES:
            logging.info(f"Batch size limit reached ({total} bytes). Remaining indices will be processed in next run.")
            break
        batch.append(idx)
        total += size
    return batch

def close_index(index):
    """Close an index on the passive cluster."""
    try:
        r = requests.post(f"{PASSIVE_CLUSTER['url']}/{index}/_close")
        r.raise_for_status()
        logging.info(f"Closed index {index}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to close index {index}: {str(e)}")
        return False

def get_index_doc_count(index, cluster_config):
    """Get document count for a specific index from the specified cluster."""
    try:
        r = requests.get(f"{cluster_config['url']}/{index}/_count")
        r.raise_for_status()
        return r.json()['count']
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get document count for {index} from {cluster_config['url']}: {str(e)}")
        return None

def verify_restore_success(index, expected_docs):
    """Verify if restore was successful by comparing document counts."""
    try:
        actual_docs = get_index_doc_count(index, PASSIVE_CLUSTER)
        if actual_docs is None:
            return False
        
        if actual_docs == expected_docs:
            logging.info(f"Document count verified for {index}: {actual_docs} documents")
            return True
        else:
            logging.warning(f"Document count mismatch for {index}: expected {expected_docs}, got {actual_docs}")
            return False
    except Exception as e:
        logging.error(f"Error verifying restore for {index}: {str(e)}")
        return False

def restore_index(snapshot, index, expected_docs):
    """Restore an index from snapshot to passive cluster."""
    try:
        # First verify the snapshot exists and is accessible
        verify_url = f"{PASSIVE_CLUSTER['url']}/_snapshot/{SNAPSHOT_REPO}/{snapshot}"
        verify_response = requests.get(verify_url)
        if not verify_response.ok:
            logging.error(f"Snapshot verification failed: {verify_response.text}")
            return False

        body = {
            "indices": index,
            "include_global_state": False,
            "include_aliases": True
        }
        
        restore_url = f"{PASSIVE_CLUSTER['url']}/_snapshot/{SNAPSHOT_REPO}/{snapshot}/_restore"
        logging.info(f"Attempting to restore index {index} from {restore_url}")
        
        r = requests.post(
            restore_url,
            json=body,
            timeout=300  # 5 minute timeout
        )
        
        if not r.ok:
            logging.error(f"Restore failed with status {r.status_code}: {r.text}")
            return False
            
        logging.info(f"Restore request accepted for index {index}")
        
        # Wait for restore to complete by checking index health
        max_attempts = 30
        attempt = 0
        while attempt < max_attempts:
            try:
                health_url = f"{PASSIVE_CLUSTER['url']}/_cluster/health/{index}"
                health_response = requests.get(health_url)
                if health_response.ok:
                    health_data = health_response.json()
                    status = health_data.get('status')
                    logging.info(f"Index {index} health check {attempt + 1}/{max_attempts}: status={status}, "
                               f"number_of_shards={health_data.get('number_of_shards')}, "
                               f"active_shards={health_data.get('active_shards')}, "
                               f"relocating_shards={health_data.get('relocating_shards')}, "
                               f"initializing_shards={health_data.get('initializing_shards')}, "
                               f"unassigned_shards={health_data.get('unassigned_shards')}")
                    
                    if status == 'green':
                        # Verify document count after restore
                        if verify_restore_success(index, expected_docs):
                            logging.info(f"Index {index} restored successfully with correct document count")
                            return True
                        else:
                            logging.warning(f"Index {index} is green but document count doesn't match. Retrying...")
                            # If document count doesn't match, we'll continue waiting
                else:
                    logging.warning(f"Health check failed for {index}: {health_response.text}")
                attempt += 1
                time.sleep(10)  # Wait 10 seconds between checks
            except requests.exceptions.RequestException as e:
                logging.warning(f"Error checking restore status for {index}: {str(e)}")
                attempt += 1
                time.sleep(10)
        
        logging.error(f"Restore operation timed out for index {index} after {max_attempts} attempts")
        return False
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to restore index {index}: {str(e)}")
        return False

def open_index(index):
    """Open an index on the passive cluster."""
    try:
        r = requests.post(f"{PASSIVE_CLUSTER['url']}/{index}/_open")
        r.raise_for_status()
        logging.info(f"Opened index {index}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to open index {index}: {str(e)}")
        return False

def wait_for_green(index, max_attempts=30):
    """Wait for an index to become green on the passive cluster."""
    attempt = 0
    while attempt < max_attempts:
        try:
            r = requests.get(
                f"{PASSIVE_CLUSTER['url']}/_cluster/health/{index}?wait_for_status=green&timeout=30s",
                timeout=35  # Slightly longer than the wait timeout
            )
            r.raise_for_status()
            if r.json()['status'] == 'green':
                logging.info(f"Index {index} is green")
                return True
            attempt += 1
            time.sleep(10)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error checking cluster health for {index}: {str(e)}")
            attempt += 1
            time.sleep(10)
    
    logging.error(f"Index {index} did not become green after {max_attempts} attempts")
    return False

def main():
    logging.info("Starting snapshot restore process")
    
    # Verify repository exists on passive cluster
    if not verify_repository(PASSIVE_CLUSTER, SNAPSHOT_REPO):
        logging.error(f"Repository not found on passive cluster ({PASSIVE_CLUSTER['url']}). Exiting.")
        return
    
    # Get latest snapshot from passive cluster
    snapshot = get_latest_snapshot()
    if not snapshot:
        logging.error("Failed to get latest snapshot. Exiting.")
        return

    # Get indices from snapshot
    snap_indices = get_snapshot_indices(snapshot)
    if not snap_indices:
        logging.error("Failed to get snapshot indices. Exiting.")
        return

    # Get indices from passive cluster
    local_indices = get_local_indices()
    if local_indices is None:
        logging.error("Failed to get local indices. Exiting.")
        return

    # Select indices to restore
    batch = pick_indices_to_restore(snap_indices, local_indices)
    if not batch:
        logging.info("No indices need to be restored")
        return

    # Restore each index
    for idx in batch:
        logging.info(f"Processing index: {idx}")
        
        # Get expected document count from active cluster
        expected_docs = get_index_doc_count(idx, ACTIVE_CLUSTER)
        if expected_docs is None:
            logging.error(f"Could not get document count for {idx} from active cluster, skipping")
            continue
            
        if idx in local_indices:
            if not close_index(idx):
                continue
                
        if restore_index(snapshot, idx, expected_docs):
            if open_index(idx):
                if not wait_for_green(idx):
                    logging.error(f"Index {idx} failed to become green")
        else:
            logging.error(f"Restore failed for {idx}")

    logging.info("Snapshot restore process completed")

if __name__ == "__main__":
    main()
