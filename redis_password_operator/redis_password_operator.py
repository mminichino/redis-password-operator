import kopf
import kubernetes
import requests
import base64
import random
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from redis_password_operator import __version__

def backoff_seconds() -> int:
    return random.randint(0, 30)

retry_strategy = Retry(
    total=10,
    backoff_factor=1,
    backoff_max=60
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

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

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, logger: logging.Logger, **_):
    settings.posting.level = logging.INFO
    try:
        logger.info(f"Starting Redis Password Operator version {__version__}")
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
