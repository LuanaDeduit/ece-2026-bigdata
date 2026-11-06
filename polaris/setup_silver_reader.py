import base64
import json
import os
import shlex
import subprocess
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

base = os.environ.get("POLARIS_URI", "http://polaris:8181")
catalog = os.environ.get("POLARIS_CATALOG", "user-l-deduit-ece")
api = base + "/api/management/v1"
headers = {"Polaris-Realm": "POLARIS"}

secret = json.loads(subprocess.check_output(
    ["kubectl", "get", "secret", "polaris-credential", "-o", "json"],
    text=True,
))
root_password = base64.b64decode(
    secret["data"]["POLARIS_ROOT_SECRET"]
).decode()

request = Request(
    base + "/api/catalog/v1/oauth/tokens",
    data=urlencode({
        "grant_type": "client_credentials",
        "client_id": "root",
        "client_secret": root_password,
        "scope": "PRINCIPAL_ROLE:ALL",
    }).encode(),
    headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
)
with urlopen(request, timeout=30) as response:
    token = json.load(response)["access_token"]

headers.update({
    "Authorization": "Bearer " + token,
    "Content-Type": "application/json",
})

def call(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(api + path, data=data, headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        content = response.read()
        return json.loads(content) if content else {}

def exists(path):
    try:
        call("GET", path)
        return True
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise

principal = "silver_only"
principal_role = "silver_only_role"
catalog_role = "silver_reader"
credentials_path = Path("/home/onyxia/.polaris-silver-credentials.env")

if not exists(f"/principals/{principal}"):
    result = call("POST", "/principals", {
        "principal": {"name": principal, "properties": {}},
        "credentialRotationRequired": False,
    })
    credentials = result["credentials"]
    value = credentials["clientId"] + ":" + credentials["clientSecret"]
    fd = os.open(credentials_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write("POLARIS_SILVER_ONLY_CREDENTIAL=" + shlex.quote(value) + "\n")
    print("Principal created; credentials saved privately.")
elif not credentials_path.exists():
    raise SystemExit("Principal exists but its local credential file is missing.")

if not exists(f"/principal-roles/{principal_role}"):
    call("POST", "/principal-roles", {
        "principalRole": {"name": principal_role, "properties": {}}
    })

role_path = f"/catalogs/{catalog}/catalog-roles"
if not exists(f"{role_path}/{catalog_role}"):
    call("POST", role_path, {
        "catalogRole": {"name": catalog_role, "properties": {}}
    })

grants_path = f"{role_path}/{catalog_role}/grants"
wanted = [
    {"type": "catalog", "privilege": "CATALOG_READ_PROPERTIES"}
]
for privilege in (
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_READ_PROPERTIES",
    "TABLE_READ_DATA",
):
    wanted.append({
        "type": "namespace",
        "namespace": ["silver"],
        "privilege": privilege,
    })

existing = call("GET", grants_path).get("grants", [])
for grant in wanted:
    if not any(
        all(item.get(key) == value for key, value in grant.items())
        for item in existing
    ):
        call("PUT", grants_path, {"grant": grant})
    print("Grant:", grant)

path = f"/principals/{principal}/principal-roles"
roles = call("GET", path).get("roles", [])
if not any(role["name"] == principal_role for role in roles):
    call("PUT", path, {"principalRole": {"name": principal_role}})

path = f"/principal-roles/{principal_role}/catalog-roles/{catalog}"
roles = call("GET", path).get("roles", [])
if not any(role["name"] == catalog_role for role in roles):
    call("PUT", path, {"catalogRole": {"name": catalog_role}})

print("RBAC ready: silver_only -> silver_only_role -> silver_reader")
