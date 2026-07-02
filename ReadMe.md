# edge-service

PLC data-acquisition layer that runs on-site (Raspberry Pi 5) and publishes
machine register data to the cloud MQTT broker over mutual TLS.

This service polls a PLC over the local factory LAN and publishes its holding
registers to MQTT. From there the cloud pipeline takes over:

```
PLC (Modbus TCP) ──LAN──> Pi (this service) ──MQTTS──> [WAN] ──> MQTT→Kafka bridge ──> Kafka ──> Go consumer ──> DB
```

---

## Why this runs on the Pi and not in ECS

The poller lives at the edge by design, not convenience:

- **The PLC is on the factory LAN** (`192.168.3.82`). That address is only
  reachable from inside the plant network. ECS lives in the AWS VPC, a separate
  network — it cannot reach a LAN IP without a VPN, and even with a VPN the
  poll loop would cross the WAN on every Modbus transaction.
- **Modbus is half-duplex request/response.** Polling across a WAN pays the
  round-trip latency on *every* transaction, sequentially. ~50 transactions at
  40 ms RTT = ~2 s per cycle just in network time. On the LAN it's sub-50 ms.
- **Resilience.** If the internet drops, the Pi keeps polling the PLC over the
  LAN and the MQTT client spools locally, draining on reconnect. A cloud poller
  loses data at the source during an outage.
- **Security.** Only the MQTT broker is internet-facing. The PLC never leaves
  the factory network.

The cloud (ECS) owns everything *behind* the MQTT broker — the bridge, the
Kafka consumers, the database. The split: edge owns physical I/O and the
unreliable link; cloud owns the durable log and processing.

---

## Prerequisites

- Raspberry Pi 5 (`aarch64` / 64-bit) running Raspberry Pi OS
- Docker + Docker Compose plugin
- Network line of sight to the PLC on the factory LAN (`ping 192.168.3.82`)
- AWS credentials scoped to pull from ECR (see [IAM](#iam-scoped-pull-user))

---

## First-time setup

### 1. Install AWS CLI v2 (ARM64)

The Pi 5 is `aarch64`, so use AWS's official ARM installer — not
`apt install awscli` (v1, outdated) or the snap (flaky on Pi).

```bash
sudo apt-get update && sudo apt-get install -y unzip curl
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version   # expect: aws-cli/2.x.x ... linux/aarch64
```

### 2. Configure credentials

```bash
aws configure                  # keys for the scoped ECR-pull IAM user
aws sts get-caller-identity    # confirm account 590183751536
```

### 3. Authenticate Docker to ECR

```bash
aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin \
      590183751536.dkr.ecr.ap-southeast-1.amazonaws.com
```

Expect `Login Succeeded`. **This token expires after 12 hours** — for
unattended operation, install the credential helper (see
[Unattended operation](#unattended-operation)) so you never run this manually
again.

### 4. Provide config and certificates

Config goes in `.env`; the MQTT client certs are mounted as files (cleaner than
stuffing multi-line PEM into env vars). Pull them from SSM once:

```bash
./load-config.sh   # see below
```

### 5. Bring the stack up

```bash
docker compose -f edge-service/docker-compose.yml up -d --build
```

---

## Configuration

### `.env`

```ini
PLC_PORT=5011
PLC_HOST=192.168.3.82
PLC_MODEL=
DEVICES_2bit=
DEVICES_16bit=D,800,1,D,820,1,D,840,1,D,3026,1,D,3028,1,D,3048,1
MQTT_TOPIC=machineh/holding_register/all/
MQTTS_ON=true
PPROFT_PORT=6060
MQTT_HOST=<broker-host-from-ssm>
MQTT_CA_PATH=/certs/ca.crt
MQTT_CLIENT_CERT_PATH=/certs/client.crt
MQTT_PRIVATE_KEY_PATH=/certs/client.key
```

`DEVICES_16bit` format is comma-separated tuples of
`type,address,count` — e.g. `D,800,1` reads one 16-bit word from `D800`.

### Certificates

The three MQTT TLS files live in `./certs/` and mount read-only into the
container at `/certs`:

| File              | SSM parameter                          |
| ----------------- | -------------------------------------- |
| `certs/ca.crt`    | `/EC2_MQTT_CA_CERTIFICATE`             |
| `certs/client.crt`| `/EC2_MQTT_CLIENT_CERTIFICATE`         |
| `certs/client.key`| `/EC2_MQTT_PRIVATE_KEY`                |

### `load-config.sh`

Pulls config and certs from SSM so nothing sensitive is hand-copied. Run once on
a machine with AWS access (the Pi itself, or a trusted workstation).

```bash
#!/usr/bin/env bash
set -euo pipefail
REGION=ap-southeast-1
mkdir -p certs

get() {
  aws ssm get-parameter --region "$REGION" --with-decryption \
    --name "$1" --query 'Parameter.Value' --output text
}

get /EC2_MQTT_CA_CERTIFICATE     > certs/ca.crt
get /EC2_MQTT_CLIENT_CERTIFICATE > certs/client.crt
get /EC2_MQTT_PRIVATE_KEY        > certs/client.key

echo "MQTT_HOST=$(get /SUPABASE/GIMDASHBOARD/MQTT/HOST)" >> .env
```

> Fetching SSM parameters needs `ssm:GetParameter` **and** `kms:Decrypt` for the
> SecureString cert params. The scoped ECR-pull user below does *not* include
> these — run `load-config.sh` with a credential that has SSM read access, then
> the Pi only needs the ECR-pull user for ongoing image pulls.

---

## docker-compose.yml

```yaml
services:
  ecs-mach-pub:
    image: 590183751536.dkr.ecr.ap-southeast-1.amazonaws.com/msp-go:2.22v.ecs
    container_name: ecs-mach-pub
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./certs:/certs:ro
    ports:
      - "6060:6060"   # pprof, optional
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

Notes on the translation from the original ECS task definition:

- **No `awslogs` driver** — there's no CloudWatch agent locally. `json-file`
  with rotation keeps the SD card from filling.
- **`restart: unless-stopped`** replaces ECS `essential: true` restart behavior.
- **No `portMappings` needed** — this is a publisher (outbound to PLC + outbound
  MQTT). Only pprof on `6060` is exposed, and only if you want it.
- **Secrets** that ECS resolved from SSM ARNs are handled here via the mounted
  `certs/` dir + `.env`, since Compose can't resolve SSM ARNs.

---

## Unattended operation

### ECR credential helper

So the Pi auto-refreshes its ECR token instead of needing a manual
`docker login` every 12 hours:

```bash
sudo apt-get install -y amazon-ecr-credential-helper
```

`~/.docker/config.json`:

```json
{
  "credHelpers": {
    "590183751536.dkr.ecr.ap-southeast-1.amazonaws.com": "ecr-login"
  }
}
```

With this, Docker fetches a fresh token from the stored AWS creds on every pull
— survives reboots, no manual login.

### IAM: scoped pull user

Do **not** put broad AWS keys on a floor device. Create a dedicated user
(e.g. `gim-pi-ecr-pull`) whose only ability is to pull this one image:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcrAuthToken",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "EcrPullMspGo",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchCheckLayerAvailability"
      ],
      "Resource": "arn:aws:ecr:ap-southeast-1:590183751536:repository/msp-go"
    }
  ]
}
```

`GetAuthorizationToken` must be `Resource: "*"` (AWS doesn't allow scoping it),
but it only mints the login token. Actual pull rights are locked to the
`msp-go` repo. Worst case if these keys leak: someone pulls that one image and
nothing else.

---

## Troubleshooting

**`no basic auth credentials` on pull**
Docker isn't authenticated to ECR. Run the `docker login` from step 3, or
install the credential helper. Not an SSH or network problem.

**`no matching manifest for linux/arm64`**
The image was built amd64-only (for the ECS platform) and has no ARM variant.
The Pi 5 needs an arm64 image — this is a fix on the *push* side (multi-arch
`docker buildx` build), not something configurable on the Pi.

**`The "FILTER" / "LOOPING" variable is not set`**
A service in compose references `${FILTER}` / `${LOOPING}` with no default and
nothing in `.env`. Harmless if those services run fine with blank values;
otherwise add them to `.env` or give defaults like `${FILTER:-}`.

**Container can't reach the PLC**
Confirm the Pi itself can reach it: `ping 192.168.3.82`. With default bridge
networking the container routes to the LAN through the Pi's interface. If
routing is odd, `network_mode: host` removes the bridge hop.

**MQTT TLS handshake fails**
Check the cert files actually populated (`ls -l certs/`, non-zero size) and that
the app is reading from the mounted `/certs` paths, not expecting PEM text in
env vars.

---

## Security checklist

- [ ] `.env` and `certs/` are in `.gitignore` — these are live MQTT credentials
- [ ] Pi uses the scoped ECR-pull IAM user, not broad keys
- [ ] SSM/KMS read access is used only at config-load time, not left on the Pi
- [ ] PLC remains on the factory LAN; only the MQTT broker is internet-facing