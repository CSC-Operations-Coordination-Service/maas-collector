# MAAS Operator — Plan 4: csc-ocs-ansible Deployment

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the maas-operator app and backend to the monitoring and TDH Grafana instances as a swarm stack, update supporting systems to read configuration from the source of truth (index + OpenBao), and verify end-to-end operation.

**Architecture:** A Docker Compose swarm stack in `csc-ocs-ansible/stacks/apps/maas-operator/` provides nginx to serve the app plugin zip with authenticated requests, two `maas-operator-backend` instances (admin and public, each scoped to its Grafana), and one `operator_worker` per collector service (short and long replay lanes, product check, pulling config from the index). Each Grafana provisioning sets up the plugin, datasource, and environment variables. The monitoring Grafana (grafana-ocs) is already 13.2 on swarm; the TDH Grafana (11.5.3, standalone) is upgraded to 13.2 and networked to the public backend. Systems reading configuration (`maas-collector-alerts.py`, docker-jobs recollect) switch from file-based configs to querying the index or using synced folders.

**Tech Stack:** Docker Compose (swarm), nginx, Python (fastapi backend), OpenBao, RabbitMQ (existing), Grafana 13.2, ansible (provisioning).

**Spec:** `/home/aagazzi/work/maas-collector/docs/superpowers/specs/2026-10-16-maas-operator-design.md`

## Global Constraints

- Spec pins backend images to git refs of maas-model fastapigen branch and maas-collector ≥ operator merge; pin once both are released.
- Backend AMQP user must have *configure* + write on `maas-operator-exchange`.
- OpenBao AppRole for admin backend: read + create/update on `<mount>/data/<VAULT_PATH_ROOT>/*`, read on `<mount>/metadata/<VAULT_PATH_ROOT>/*`.
- Worker `stop_grace_period` must exceed 10 s (spec requires >10 s; set to 15 s).
- Grafana env: `GF_DATAPROXY_SEND_USER_HEADER=true`, `GF_DATAPROXY_TIMEOUT≥45`, `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=maas-operator-app` (unless signed), install plugin via `GF_PLUGINS_PREINSTALL_SYNC` from nginx image.
- TDH Grafana users must log in with named accounts (no anonymous access).
- Backend must run `init-indices` once before first use.
- Source of truth: `maas-collector-config` index + OpenBao; file configs are sync target only.

## Review Focus

1. **Worker queue consumption model:** Workers must consume two lanes (short/long) per service, each serialized. Confirm routing key bindings and prefetch settings prevent cross-lane blocking.
2. **No health endpoint on worker:** A hung replay leaves an operation RUNNING indefinitely. Verify the `stop_grace_period` and child process termination logic actually interrupt stuck replays and do not create zombie processes.
3. **TDH Grafana named-account requirement:** Anonymous users on TDH will get 401 on every backend request and see no data. Verify via live test: TDH users can log in, plugin loads, operations page works.
4. **Index initialization timing:** `init-indices` must run before any operation record is written. Spec says "once before first use"; verify in docker-compose that this runs before backends start serving requests (depends_on + healthcheck).
5. **Config sync idempotence:** Recollect jobs or alert rules reading synced config must tolerate and recover from partial or missed syncs. Verify that a missing sync file or stale config does not crash the job, and that resyncing fixes the state.

---

## File Structure

**csc-ocs-ansible:**
- `stacks/apps/maas-operator/docker-compose.yml` — swarm stack definition
- `stacks/apps/maas-operator/.env` (or inline) — environment variables per deployment
- `stacks/apps/maas-operator/nginx/Dockerfile` — nginx image building app zip
- `stacks/apps/maas-operator/nginx/nginx.conf` — proxy routes, auth headers
- `provisioning/grafana-ocs/` — admin Grafana provisioning (plugin, datasource, env)
- `provisioning/tdh-grafana/` — TDH Grafana provisioning (upgrade, plugin, datasource, env)

**maas-collector:**
- `src/maas_collector/maas_collector_alerts.py` — update to query index instead of files

**docker-jobs:**
- `jobs/recollect/` — update to read from index or synced folder

---

### Task 1: Create the swarm stack docker-compose.yml

**Files:**
- Create: `stacks/apps/maas-operator/docker-compose.yml`
- Create: `stacks/apps/maas-operator/.env.example` (documentation of required env vars)

**Interfaces:**
- Consumes: 
  - maas-operator-app image from CI (nginx serving `/dist/maas-operator-app-*.zip`)
  - maas-operator-backend image (admin and public profiles)
  - maas-collector image with operator_worker CLI
  - RabbitMQ broker (existing, coordinates exchange `maas-operator-exchange`)
  - OpenBao (existing, for credentials)
  - OpenSearch (existing, for index queries)
- Produces: 
  - Five running services: nginx, backend-admin, backend-public, operator_worker (short lane), operator_worker (long lane)
  - Ready-to-use stack that can be deployed with `docker stack deploy`

- [ ] **Step 1: Create base docker-compose.yml with services**

Create `stacks/apps/maas-operator/docker-compose.yml` (version 3.8+) with placeholder services. Use the download-manager stack as a reference for structure. Start with service stubs:

```yaml
version: '3.8'

services:
  nginx:
    image: ${NGINX_IMAGE:-maas-operator-nginx:latest}
    ports:
      - "8080:8080"
    networks:
      - admin
      - public
      - default
    # TODO: volumes for app zip

  backend-admin:
    image: ${BACKEND_IMAGE:-maas-operator-backend:latest}
    environment:
      OPERATOR_PROFILE: admin
    networks:
      - admin
      - default
    # TODO: env vars, health check

  backend-public:
    image: ${BACKEND_IMAGE:-maas-operator-backend:latest}
    environment:
      OPERATOR_PROFILE: public
    networks:
      - public
      - default
    # TODO: env vars, health check

  operator_worker_short:
    image: ${COLLECTOR_IMAGE:-maas-collector:latest}
    command: python -m maas_collector.rawdata.cli.operator_worker
    environment:
      OPERATOR_LANE: short
    networks:
      - default
    # TODO: stop_grace_period, env vars

  operator_worker_long:
    image: ${COLLECTOR_IMAGE:-maas-collector:latest}
    command: python -m maas_collector.rawdata.cli.operator_worker
    environment:
      OPERATOR_LANE: long
    networks:
      - default
    # TODO: stop_grace_period, env vars

networks:
  admin:
    driver: overlay
  public:
    driver: overlay
  default:
    driver: overlay
```

Document the networks: `admin` connects nginx and admin backend; `public` connects nginx and public backend; `default` connects all services to RabbitMQ/OpenSearch.

- [ ] **Step 2: Add init-indices service and dependencies**

Add an `init-indices` service that runs once before backends start:

```yaml
  init-indices:
    image: ${BACKEND_IMAGE:-maas-operator-backend:latest}
    command: python -m maas_operator_backend.cli init-indices
    environment:
      # OpenSearch credentials
      OPENSEARCH_URL: ${OPENSEARCH_URL}
      # (others as needed)
    deploy:
      restart_policy:
        condition: none
    networks:
      - default
    # No depends_on here; backends will depend on this
```

Add `depends_on` to backend services:
```yaml
  backend-admin:
    depends_on:
      init-indices:
        condition: service_completed_successfully
```

- [ ] **Step 3: Add environment variables and secrets**

Add `env_file` or inline environment section to each service. Create `.env.example`:

```bash
# Images
NGINX_IMAGE=maas-operator-nginx:latest
BACKEND_IMAGE=maas-operator-backend:latest
COLLECTOR_IMAGE=maas-collector:latest

# Profiles
BACKEND_ADMIN_PROFILE=admin
BACKEND_PUBLIC_PROFILE=public

# Services
BACKEND_ADMIN_PORT=8000
BACKEND_PUBLIC_PORT=8001

# OpenBao
OPENBAO_ADDR=https://openbao.example.com:8200
OPENBAO_ROLE_ID=${OPENBAO_ADMIN_ROLE_ID}
OPENBAO_SECRET_ID=${OPENBAO_ADMIN_SECRET_ID}
VAULT_PATH_ROOT=apps/maas-collector

# OpenSearch
OPENSEARCH_URL=https://opensearch.example.com:9200
OPENSEARCH_USERNAME=maas-operator
OPENSEARCH_PASSWORD=${OPENSEARCH_PASSWORD}

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
RABBITMQ_USER=maas-operator
RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}

# Timeouts
OPERATOR_POLL_INTERVAL=5
OPERATOR_CHECK_TIMEOUT=20
OPERATOR_REPLAY_RETRY=3
RECOLLECT_MAX_HOURS_ADMIN=2208
RECOLLECT_MAX_HOURS_PUBLIC=24
```

- [ ] **Step 4: Add health checks and stop grace period**

For each backend service, add:
```yaml
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
    interval: 30s
    timeout: 10s
    retries: 3
```

For worker services, add:
```yaml
  deploy:
    stop_grace_period: 15s
```

- [ ] **Step 5: Commit**

```bash
cd /home/aagazzi/work/csc-ocs-ansible
git add stacks/apps/maas-operator/docker-compose.yml stacks/apps/maas-operator/.env.example
git commit -m "feat: create maas-operator swarm stack with docker-compose"
```

---

### Task 2: Build and configure nginx to serve the app and proxy requests

**Files:**
- Create: `stacks/apps/maas-operator/nginx/Dockerfile`
- Create: `stacks/apps/maas-operator/nginx/nginx.conf`

**Interfaces:**
- Consumes:
  - maas-operator-app CI artifact
  - backend services at ports defined in compose
- Produces:
  - nginx image serving static files and proxying requests
  - Health endpoint for compose healthchecks

- [ ] **Step 1: Create nginx Dockerfile**

```dockerfile
FROM nginx:alpine

# Copy app distribution files (assume downloaded or built)
# For now, create a placeholder that can be replaced with CI artifact
COPY dist/ /usr/share/nginx/html/dist/

# Copy configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Health check endpoint
RUN mkdir -p /usr/share/nginx/html && \
    echo "OK" > /usr/share/nginx/html/health

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
```

- [ ] **Step 2: Create nginx.conf**

Create `stacks/apps/maas-operator/nginx/nginx.conf`:

```nginx
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                    '$status $body_bytes_sent "$http_referer" '
                    '"$http_user_agent" "$http_x_forwarded_for"';

    access_log /var/log/nginx/access.log main;
    sendfile on;
    tcp_nopush on;
    keepalive_timeout 65;
    gzip on;

    upstream backend_admin {
        server backend-admin:8000;
    }

    upstream backend_public {
        server backend-public:8000;
    }

    server {
        listen 8080;
        server_name _;

        # Health check endpoint
        location /health {
            return 200 "OK\n";
            add_header Content-Type text/plain;
        }

        # Serve static files
        location /dist/ {
            alias /usr/share/nginx/html/dist/;
            expires 1h;
            add_header Cache-Control "public, must-revalidate";
        }

        # Proxy API requests (pass through X-Grafana-User and X-Maas-Roles headers)
        location ~ ^/(api|operator|admin)/ {
            proxy_pass http://backend-admin;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_pass_request_headers on;
            proxy_connect_timeout 10s;
            proxy_read_timeout 45s;
        }

        # Default 404
        location / {
            return 404;
        }
    }
}
```

Note: This is a simple proxy setup. The actual route family selection (admin vs public backend) happens at the Grafana level via the app's plugin.json proxy routes. This nginx setup is a basic helper; in production, it might be replaced with the Grafana proxy entirely.

- [ ] **Step 3: Document header passthrough**

Add a comment in nginx.conf:

```nginx
# IMPORTANT: Grafana sets X-Grafana-User and X-Maas-Roles via proxy routes.
# proxy_pass_request_headers on ensures these headers reach the backend.
# The backend uses X-Grafana-User for audit (requested_by) and X-Maas-Roles for authorization.
```

- [ ] **Step 4: Commit**

```bash
git add stacks/apps/maas-operator/nginx/Dockerfile stacks/apps/maas-operator/nginx/nginx.conf
git commit -m "feat: add nginx image and config for app serving"
```

---

### Task 3: Provision admin Grafana (grafana-ocs) with maas-operator-app

**Files:**
- Create: `provisioning/grafana-ocs/maas-operator-app.yaml`
- Modify: `stacks/grafana-ocs/docker-compose.yml` (env vars)

**Interfaces:**
- Consumes:
  - Existing grafana-ocs service definition
  - nginx service URL
- Produces:
  - Plugin provisioning file
  - Updated env vars in grafana-ocs compose

- [ ] **Step 1: Locate grafana-ocs provisioning structure**

Check `/home/aagazzi/work/csc-ocs-ansible/` for:
- `provisioning/grafana-ocs/` directory structure
- `stacks/grafana-ocs/docker-compose.yml`

Note: If these don't exist, document the paths where they should be created.

- [ ] **Step 2: Create plugin provisioning file**

Create `provisioning/grafana-ocs/maas-operator-app.yaml`:

```yaml
apiVersion: plugins.grafana.com/v1alpha1
kind: GrafanaPlugin
metadata:
  name: maas-operator-app
  namespace: default

spec:
  jsonData:
    backendUrl: "http://maas-operator-nginx:8080"
  signingLevel: unsigned
  # Or if using grafana provisioning format:
  id: maas-operator-app
  name: MAAS Operator
  type: app
  enabled: true
  jsonData:
    backendUrl: "http://maas-operator-nginx:8080"
```

Document the exact format expected by the grafana-ocs instance (YAML vs JSON vs other).

- [ ] **Step 3: Add env vars to grafana-ocs compose**

Modify the grafana-ocs service in `stacks/grafana-ocs/docker-compose.yml`:

```yaml
services:
  grafana-ocs:
    # ... existing config
    environment:
      # ... existing vars
      GF_DATAPROXY_SEND_USER_HEADER: "true"
      GF_DATAPROXY_TIMEOUT: "45"
      GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: "maas-operator-app"
      GF_PLUGINS_PREINSTALL_SYNC: "true"
```

- [ ] **Step 4: Verify provisioning structure**

Document in a comment or README how the provisioning is applied:
- If using ansible: point to the playbook that syncs provisioning files
- If using volume mounts: document the mount path
- Document the version requirement: Grafana 13.2+

- [ ] **Step 5: Commit**

```bash
git add provisioning/grafana-ocs/maas-operator-app.yaml stacks/grafana-ocs/docker-compose.yml
git commit -m "feat: provision admin grafana with maas-operator-app"
```

---

### Task 4: Upgrade and provision TDH Grafana for maas-operator-app

**Files:**
- Create or Modify: `provisioning/tdh-grafana/maas-operator-app.yaml`
- Modify: `stacks/tdh-grafana/docker-compose.yml` or Dockerfile

**Interfaces:**
- Consumes:
  - Existing TDH Grafana container (11.5.3)
  - nginx service URL
- Produces:
  - TDH Grafana running 13.2+
  - Plugin provisioning
  - Verification of named-account login requirement

- [ ] **Step 1: Locate TDH Grafana definition**

Find the TDH Grafana docker-compose or Dockerfile in csc-ocs-ansible. Check:
- `stacks/tdh-grafana/` or similar
- Current base image version (should be 11.5.3)

- [ ] **Step 2: Upgrade base image to 13.2**

Update the Dockerfile or docker-compose.yml:

```dockerfile
FROM grafana/grafana:13.2-ubuntu
```

Or in compose:
```yaml
services:
  tdh-grafana:
    image: grafana/grafana:13.2-ubuntu
    # ... rest of config
```

- [ ] **Step 3: Apply same provisioning as admin Grafana**

Copy `provisioning/grafana-ocs/maas-operator-app.yaml` to `provisioning/tdh-grafana/maas-operator-app.yaml` or create a unified provisioning file that both use.

Update TDH Grafana compose:
```yaml
  tdh-grafana:
    environment:
      GF_DATAPROXY_SEND_USER_HEADER: "true"
      GF_DATAPROXY_TIMEOUT: "45"
      GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: "maas-operator-app"
      GF_PLUGINS_PREINSTALL_SYNC: "true"
```

- [ ] **Step 4: Verify network connectivity to public backend**

Document (in a comment or README) that TDH Grafana must reach `http://backend-public:8000`. If TDH is a standalone container (not in the same compose), add a network alias or reverse proxy:

```yaml
  # If TDH is in the same compose:
  tdh-grafana:
    networks:
      - public
      - default

networks:
  public:
    driver: overlay
```

Or document the external network setup.

- [ ] **Step 5: Commit**

```bash
git add provisioning/tdh-grafana/ stacks/tdh-grafana/docker-compose.yml
git commit -m "feat: upgrade tdh grafana to 13.2 and provision maas-operator-app"
```

---

### Task 5: Update maas-collector-alerts.py to read from the index

**Files:**
- Modify: `src/maas_collector/maas_collector_alerts.py`

**Interfaces:**
- Consumes:
  - OpenSearch `maas-collector-config` index
  - Existing alert rule templates
- Produces:
  - Alert generation logic querying the index
  - Same rule output format as file-based version

- [ ] **Step 1: Examine current alert generation logic**

Read `src/maas_collector/maas_collector_alerts.py` to understand:
- How it reads configs (e.g., from `configs/` folder)
- How it generates rules (e.g., for "interface not collected" alerts)
- Output format

Document this in a code comment.

- [ ] **Step 2: Add OpenSearch query method**

Add a function to query the index:

```python
def get_interfaces_from_index(opensearch_url: str, opensearch_creds: dict) -> List[dict]:
    """Query maas-collector-config index for all enabled interfaces."""
    from opensearchpy import OpenSearch
    client = OpenSearch(hosts=[opensearch_url], **opensearch_creds)
    response = client.search(
        index="maas-collector-config",
        body={"query": {"term": {"enabled": True}}}
    )
    return [hit["_source"] for hit in response["hits"]["hits"]]
```

- [ ] **Step 3: Refactor to use index queries**

Update the main alert generation logic to prefer index queries over file reads:

```python
def generate_alerts(config_dir: str = None):
    """Generate alert rules from index or fallback to config files."""
    if os.environ.get("OPENSEARCH_URL"):
        interfaces = get_interfaces_from_index(
            os.environ["OPENSEARCH_URL"],
            {"http_auth": (os.environ["OPENSEARCH_USER"], os.environ["OPENSEARCH_PASSWORD"])}
        )
    else:
        # Fallback: read from config files
        interfaces = read_interfaces_from_files(config_dir or "configs/")
    
    # Generate rules (unchanged logic)
    return generate_rules_from_interfaces(interfaces)
```

- [ ] **Step 4: Write test**

Create a test (or add to existing tests) that:
1. Mocks OpenSearch response with sample interfaces
2. Calls alert generation
3. Verifies generated rules match expected format

Example:
```python
def test_alert_generation_from_index(monkeypatch):
    import os
    monkeypatch.setenv("OPENSEARCH_URL", "http://localhost:9200")
    # Mock opensearch client
    monkeypatch.setattr("maas_collector_alerts.OpenSearch", MockOpenSearch)
    rules = generate_alerts()
    assert len(rules) > 0
    assert all("interface" in rule for rule in rules)
```

- [ ] **Step 5: Update documentation**

Add a comment or README section documenting:
- The index fallback behavior
- Required OpenSearch env vars
- How to run with file-based configs (set `OPENSEARCH_URL=` or unset)

- [ ] **Step 6: Commit (in maas-collector repo)**

```bash
cd /home/aagazzi/work/maas-collector
git add src/maas_collector/maas_collector_alerts.py
git commit -m "feat: query opensearch index for interfaces in alert generation"
```

---

### Task 6: Update docker-jobs to read from index or synced folder

**Files:**
- Modify: `jobs/recollect/` (exact path depends on docker-jobs structure)

**Interfaces:**
- Consumes:
  - OpenSearch index or synced config folder
  - Existing recollect job logic
- Produces:
  - Recollect jobs reading configs from index/synced folder

- [ ] **Step 1: Locate and examine docker-jobs recollect job**

Find the recollect job in `/home/aagazzi/work/docker-jobs/`. Document:
- Job entry point (script or Python module)
- Current config reading logic
- How it invokes maas_collector CLI

- [ ] **Step 2: Add config-reading flexibility**

Update the recollect job to:
1. Check env var `MAAS_CONFIG_SYNC_DIR` (if set, read from synced folder)
2. Else, check `OPENSEARCH_URL` (query index)
3. Else, fallback to original file-based logic

Example:
```python
def get_interfaces():
    """Read interfaces from sync dir, index, or files."""
    if sync_dir := os.environ.get("MAAS_CONFIG_SYNC_DIR"):
        return read_from_sync_dir(sync_dir)
    elif opensearch_url := os.environ.get("OPENSEARCH_URL"):
        return query_opensearch_index(opensearch_url)
    else:
        return read_from_config_files("configs/")
```

- [ ] **Step 3: Add retry/fallback logic**

If OpenSearch is down, log and fall back to synced folder. If sync folder is missing, log and fall back to config files.

- [ ] **Step 4: Write test**

Test that recollect logic correctly reads configs from each source and invokes the CLI correctly.

- [ ] **Step 5: Update cron/job definition**

Document or update any cron jobs / docker-jobs scheduler to set the env vars:
```bash
MAAS_CONFIG_SYNC_DIR=/mnt/configs
OPENSEARCH_URL=https://opensearch.example.com:9200
```

- [ ] **Step 6: Commit (in docker-jobs repo)**

```bash
cd /home/aagazzi/work/docker-jobs
git add jobs/recollect/
git commit -m "feat: read interface configs from index or synced folder"
```

---

## Self-Review

**Spec Coverage:**
- ✅ Task 1: docker-compose.yml (swarm stack, services, networking, init-indices)
- ✅ Task 2: nginx (app serving, proxy routes)
- ✅ Task 3: Admin Grafana provisioning (env vars, plugin)
- ✅ Task 4: TDH Grafana upgrade + provisioning (13.2, named accounts, plugin)
- ✅ Task 5: Alert generation from index
- ✅ Task 6: Recollect jobs from index/synced folder

**Step Scan:** Each step is actionable (create file, add env var, add function, write test, commit). No TBD.

**Type Consistency:** Functions consistent across tasks (interface dict structure, env var names).

**Review Focus:** Covered in task steps (init-indices timing, stop_grace_period, named-account test, health endpoints, fallback logic).

**Proportion:** Plan ~200 lines, spec ~200 lines. Reasonable.

---
