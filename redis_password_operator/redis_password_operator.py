import kopf
import kubernetes
import requests
import base64
import random
import warnings
import time
from datetime import datetime, timezone
from typing import Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from redis_password_operator import __version__

def backoff_seconds() -> int:
    return random.randint(0, 30)

warnings.filterwarnings("ignore")
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
retry_strategy = Retry(
    total=10,
    backoff_factor=1,
    backoff_max=60
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

ANNOTATION_KEY = "reconcile.util.redislabs.com/last-updated"
LABEL_KEY = "reconcile.util.redislabs.com/managed"

def update_label(name: str, namespace: str, key: str, value: str, logger: logging.Logger) -> bool:
    try:
        v1 = kubernetes.client.CoreV1Api()
        secret = v1.read_namespaced_secret(name, namespace)
        labels = secret.metadata.labels or {}
        if labels.get(key) == value:
            return True
        labels[key] = value
        secret.metadata.labels = labels
        v1.patch_namespaced_secret(name, namespace, secret)
        logger.info(f"Labeled secret {name} with {key}={value}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

def has_secret_key(name: str, namespace: str, key: str, logger: logging.Logger) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        return key in secret.data
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

def get_secret_key(name: str, namespace: str, key: str, logger: logging.Logger) -> Union[str, None]:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        value = secret.data.get(key)
        if value:
            return base64.b64decode(value).decode('utf-8')
        return None
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return None

def get_secret_keys(name: str, namespace: str, logger: logging.Logger) -> dict:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        return {k: base64.b64decode(v).decode('utf-8') for k, v in secret.data.items()}
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return {}

def delete_secret_key(name: str, namespace: str, key: str, logger: logging.Logger) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        if key in secret.data:
            secret.data[key] = None
            v1.patch_namespaced_secret(name, namespace, secret)
            logger.info(f"Deleted key {key} from secret {name} in {namespace}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

def create_secret(name: str, namespace: str, keys: dict, logger: logging.Logger) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        new_secret = kubernetes.client.V1Secret(
            metadata=kubernetes.client.V1ObjectMeta(name=name, namespace=namespace),
            data={key: base64.b64encode(value.encode('utf-8')).decode('utf-8') for key, value in keys.items()}
        )
        v1.create_namespaced_secret(namespace, new_secret)
        logger.info(f"Created secret {name} in {namespace}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

def set_secret_keys(name: str, namespace: str, keys: dict, logger: logging.Logger, annotate: bool = False) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        if not secret.data:
            secret.data = {}
        for key, value in keys.items():
            secret.data[key] = base64.b64encode(value.encode('utf-8')).decode('utf-8')
        if annotate:
            annotations = secret.metadata.annotations or {}
            annotations[ANNOTATION_KEY] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            secret.metadata.annotations = annotations
        v1.patch_namespaced_secret(name, namespace, secret)
        logger.info(f"Updated secret {name} in {namespace}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            return create_secret(name, namespace, keys, logger)
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

@kopf.on.create('util.redislabs.com', 'v1', 'redisclusterpasswords')
@kopf.on.update('util.redislabs.com', 'v1', 'redisclusterpasswords')
def update_redis_password(spec, name, namespace, logger, **_):
    logger.info(f"Processing {name} in {namespace}")
    secret_spec = spec.get('secret')
    rec_spec = spec.get('rec')

    if not secret_spec or not rec_spec:
        raise kopf.PermanentError("Spec must contain 'secret' and 'rec' blocks")

    src_secret_name = secret_spec.get('name')
    src_secret_namespace = secret_spec.get('namespace')
    
    rec_name = rec_spec.get('name')
    rec_username = rec_spec.get('username')
    rec_namespace = rec_spec.get('namespace')

    if not update_label(src_secret_name, src_secret_namespace, LABEL_KEY, 'true', logger):
        raise kopf.TemporaryError(f"Failed to update label on source secret {src_secret_name} in {src_secret_namespace}.", delay=15)

    new_password = get_secret_key(src_secret_name, src_secret_namespace, 'password', logger)
    if not new_password:
        raise kopf.TemporaryError(f"Source secret {src_secret_name} in {src_secret_namespace} does not contain 'password' field", delay=15)

    old_password = get_secret_key(rec_name, rec_namespace, 'password', logger)
    if not old_password:
        logger.warning(f"Target secret {rec_name} in {rec_namespace} does not contain 'password' field.")

    if not rec_username:
        rec_username = get_secret_key(rec_name, rec_namespace, 'username', logger)
        if not rec_username:
            logger.warning(f"Target secret {rec_name} in {rec_namespace} does not contain 'username' field.")

    if old_password == new_password:
        logger.info(f"{rec_name}: Passwords are the same, skipping update.")
        return

    url = f"https://{rec_name}.{rec_namespace}.svc.cluster.local:9443/v1/users/password"
    
    payload = {
        "username": rec_username,
        "old_password": old_password,
        "new_password": new_password
    }
    
    logger.info(f"Updating password for user {rec_username}")
    
    try:
        response = session.post(
            url,
            auth=(rec_username, old_password),
            json=payload,
            verify=False,
            headers={'Content-Type': 'application/json'},
            timeout=5
        )
        response.raise_for_status()
        logger.info("Successfully updated password via REST API")
    except requests.exceptions.RequestException as e:
        raise kopf.TemporaryError(f"REST API call failed: {e}", delay=15)

    data = {
        'username': rec_username,
        'password': new_password
    }
    if not set_secret_keys(rec_name, rec_namespace, data, logger):
        raise kopf.TemporaryError(f"Failed to update secret {rec_name} in {rec_namespace}", delay=15)

    delete_old_password(rec_username, old_password, rec_name, rec_namespace, new_password, logger)

    logger.info(f"Successfully updated secret {rec_name} in {rec_namespace}")

def delete_old_password(rec_username, old_password, rec_name, rec_namespace, rec_password, logger):
    remaining = 300
    logger.info(f"Waiting for old password deletion window ({remaining}s remaining).")
    time.sleep(remaining)

    logger.info(f"Deleting old password for {rec_name} in {rec_namespace}.")

    payload = {
        "username": rec_username,
        "old_password": old_password
    }

    url = f"https://{rec_name}.{rec_namespace}.svc.cluster.local:9443/v1/users/password"
    try:
        response = session.delete(
            url,
            auth=(rec_username, rec_password),
            json=payload,
            verify=False,
            headers={'Content-Type': 'application/json'},
            timeout=5
        )
        response.raise_for_status()
        logger.info("Successfully deleted old password via REST API")
    except requests.exceptions.RequestException as e:
        if e.response.status_code == 400:
            logger.warning("Can not delete old password via REST API, old password does not exist.")
        else:
            raise kopf.TemporaryError(f"REST API call failed: {e}", delay=15)

    logger.info(f"Deleted old password for {rec_name}.")

@kopf.on.create('v1', 'secrets', labels={LABEL_KEY: 'true'})
@kopf.on.update('v1', 'secrets', labels={LABEL_KEY: 'true'})
def watch_secret_updates(meta, logger, **_):
    name = meta['name']
    namespace = meta['namespace']

    custom_api = kubernetes.client.CustomObjectsApi()

    try:
        rcps = custom_api.list_cluster_custom_object(
            group="util.redislabs.com",
            version="v1",
            plural="redisclusterpasswords"
        )

        for rcp in rcps.get('items', []):
            spec = rcp.get('spec', {})
            src_secret = spec.get('secret', {})

            if src_secret.get('name') == name and src_secret.get('namespace') == namespace:
                logger.info(f"Secret {name} in {namespace} changed. Triggering update for {rcp['metadata']['name']}.")

                now = datetime.now(timezone.utc).isoformat()
                custom_api.patch_namespaced_custom_object(
                    group="util.redislabs.com",
                    version="v1",
                    namespace=rcp['metadata']['namespace'],
                    plural="redisclusterpasswords",
                    name=rcp['metadata']['name'],
                    body={
                        "metadata": {
                            "annotations": {
                                ANNOTATION_KEY: now
                            }
                        }
                    }
                )
    except kubernetes.client.exceptions.ApiException as e:
        raise kopf.TemporaryError(f"Failed to list RedisClusterPasswords: {e}", delay=15)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, logger: logging.Logger, **_):
    settings.posting.level = logging.INFO
    settings.networking.error_backoffs = [10, 20, 30]
    settings.batching.error_delays = [10, 20, 30]
    settings.persistence.progress_storage = kopf.AnnotationsProgressStorage(prefix='status.util.redislabs.com')
    settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(prefix='status.util.redislabs.com')
    try:
        logger.info(f"Starting Redis Password Operator version {__version__}")
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
