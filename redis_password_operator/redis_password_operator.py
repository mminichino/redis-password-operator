import kopf
import kubernetes
import requests
import base64
import random
import time
import warnings
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

def delete_secret_key(name: str, namespace: str, key: str, logger: logging.Logger) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        if key in secret.data:
            del secret.data[key]
            v1.patch_namespaced_secret(name, namespace, secret)
            logger.info(f"Deleted key {key} from secret {name} in {namespace}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

def set_secret_keys(name: str, namespace: str, keys: dict, logger: logging.Logger) -> bool:
    v1 = kubernetes.client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
        if not secret.data:
            secret.data = {}
        for key, value in keys.items():
            secret.data[key] = base64.b64encode(value.encode('utf-8')).decode('utf-8')
        v1.patch_namespaced_secret(name, namespace, secret)
        logger.info(f"Updated secret {name} in {namespace}.")
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Secret {name} in {namespace} not found.")
        else:
            logger.error(f"Can not connect to Kubernetes API, status {e.status}")
        return False

@kopf.on.create('util.redislabs.com', 'v1', 'redisclusterpasswords')
@kopf.on.update('util.redislabs.com', 'v1', 'redisclusterpasswords')
def update_redis_password(spec, name, namespace, logger, **kwargs):
    logger.debug(f"Processing {name} in {namespace} kwarg: {kwargs}")
    secret_spec = spec.get('secret')
    rec_spec = spec.get('rec')

    if not secret_spec or not rec_spec:
        raise kopf.PermanentError("Spec must contain 'secret' and 'rec' blocks")

    src_secret_name = secret_spec.get('name')
    src_secret_namespace = secret_spec.get('namespace')
    
    rec_name = rec_spec.get('name')
    rec_username = rec_spec.get('username')
    rec_namespace = rec_spec.get('namespace')

    v1 = kubernetes.client.CoreV1Api()

    try:
        src_secret = v1.read_namespaced_secret(src_secret_name, src_secret_namespace)

        update_label(src_secret_name, src_secret_namespace, 'reconcile.util.redislabs.com/managed', 'true', logger)

        if 'password' not in src_secret.data:
            logger.warning(f"Source secret {src_secret_name} in {src_secret_namespace} does not contain 'password' field.")
            raise kopf.TemporaryError(f"Secret {src_secret_name} in {src_secret_namespace} does not contain 'password' field", delay=15)
        new_password = base64.b64decode(src_secret.data['password']).decode('utf-8')
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Source secret {src_secret_name} in {src_secret_namespace} not found.")
            raise kopf.TemporaryError(f"Source secret {src_secret_name} in {src_secret_namespace} not found", delay=15)
        raise kopf.TemporaryError(f"Can not connect to Kubernetes API, status {e.status}", delay=15)

    try:
        target_secret = v1.read_namespaced_secret(rec_name, rec_namespace)
        if not target_secret.data or 'password' not in target_secret.data:
            logger.warning(f"Target secret {rec_name} in {rec_namespace} does not contain 'password' field.")
            raise kopf.TemporaryError(f"Secret {rec_name} in {rec_namespace} does not contain 'password' field", delay=15)
        old_password = base64.b64decode(target_secret.data['password']).decode('utf-8')
        if not rec_username:
            if not target_secret.data or 'username' not in target_secret.data:
                logger.warning(f"Target secret {rec_name} in {rec_namespace} does not contain 'username' field.")
                raise kopf.TemporaryError(f"Secret {rec_name} in {rec_namespace} does not contain 'username' field", delay=15)
            rec_username = base64.b64decode(target_secret.data['username']).decode('utf-8')
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning(f"Target secret {rec_name} in {rec_namespace} not found.")
            raise kopf.TemporaryError(f"Secret {rec_name} in {rec_namespace} not found", delay=15)
        raise kopf.TemporaryError(f"Can not connect to Kubernetes API, status {e.status}", delay=15)

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

    encoded_new_password = base64.b64encode(new_password.encode('utf-8')).decode('utf-8')
    encoded_username = base64.b64encode(rec_username.encode('utf-8')).decode('utf-8')
    
    if target_secret:
        if not target_secret.data:
            target_secret.data = {}
        target_secret.data['username'] = encoded_username
        target_secret.data['password'] = encoded_new_password
        v1.replace_namespaced_secret(rec_name, rec_namespace, target_secret)
    else:
        new_secret = kubernetes.client.V1Secret(
            metadata=kubernetes.client.V1ObjectMeta(name=rec_name, namespace=rec_namespace),
            data={
                'username': rec_username,
                'password': encoded_new_password
            }
        )
        v1.create_namespaced_secret(rec_namespace, new_secret)
    
    logger.info("Waiting 5 minutes to delete old password.")
    time.sleep(300)

    payload = {
        "username": rec_username,
        "old_password": old_password
    }

    try:
        response = session.delete(
            url,
            auth=(rec_username, new_password),
            json=payload,
            verify=False,
            headers={'Content-Type': 'application/json'},
            timeout=5
        )
        response.raise_for_status()
        logger.info("Successfully deleted old password via REST API")
    except requests.exceptions.RequestException as e:
        raise kopf.TemporaryError(f"REST API call failed: {e}", delay=15)

    logger.info(f"Successfully updated secret {rec_name} in {rec_namespace}")

@kopf.on.event('', 'v1', 'secrets', labels={'reconcile.util.redislabs.com/managed': 'true'})
def watch_secret_updates(event, name, namespace, logger, **kwargs):
    logger.debug(f"Processing secret event {event['type']} for {name} in {namespace} kwarg: {kwargs}")
    if event['type'] != 'MODIFIED':
        return

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

                update_redis_password(
                    spec=spec,
                    name=rcp['metadata']['name'],
                    namespace=rcp['metadata']['namespace'],
                    logger=logger
                )
    except kubernetes.client.exceptions.ApiException as e:
        raise kopf.TemporaryError(f"Failed to list RedisClusterPasswords: {e}", delay=15)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, logger: logging.Logger, **_):
    settings.posting.level = logging.INFO
    try:
        logger.info(f"Starting Redis Password Operator version {__version__}")
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
