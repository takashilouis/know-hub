# Running Morphik Core Locally

This guide explains how to run Morphik Core locally using Docker.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
- `git` for cloning the repository.
- `Python 3.11+` (optional, for running helper scripts locally).

## Configuration

The main configuration file is `morphik.toml`.

### Key Settings for Docker
When running in Docker, ensure your `morphik.toml` uses container service names instead of `localhost` for internal connections:

```toml
[redis]
url = "redis://redis:6379/0"
host = "redis"

[completion]
# Use 'ollama' hostname when connecting from inside Docker
model = "ollama_qwen_vision" 

[registered_models]
# Ollama models should point to http://ollama:11434
ollama_qwen_vision = { model_name = "ollama_chat/qwen2.5vl:latest", api_base = "http://ollama:11434", vision = true }
```

## Running with Docker Compose

The easiest way to run the entire stack (Morphik API, Worker, Redis, Postgres, Ollama) is with Docker Compose.

### Option 1: Using the helper script (Recommended)
This script automatically detects the port from `morphik.toml` and starts the services.

```bash
./start-dev.sh
```

### Option 2: Using Docker Compose directly

```bash
docker compose up -d
```

## Verifying deployment

1. **Check Containers**: Ensure all containers are running.
   ```bash
   docker compose ps
   ```

2. **Check Logs**:
   - **API**: `docker logs -f morphik-core-morphik-1`
   - **Worker**: `docker logs -f morphik-core-worker-1`

3. **Access the API**:
   The API will be available at `http://localhost:8000` (or the port defined in `morphik.toml`).
   - Health check: `http://localhost:8000/health`
   - API Docs: `http://localhost:8000/docs`

## Troubleshooting

### "Connection refused" to Redis or Ollama
If you see errors like `Connection refused` connecting to `localhost:6379` or `localhost:11434` in the logs:
- **Cause**: The services connect to `localhost` which refers to the container itself, not the other services.
- **Fix**: Update `morphik.toml` to use `redis` and `ollama` as hostnames (as shown in Configuration section).

### "No space left on device"
If Postgres fails to start with this error:
- **Fix**: Prune unused Docker data:
  ```bash
  docker system prune -a --volumes
  ```

## Running the Frontend

The frontend is located in `ee/ui-component`.

### 1. Setup

```bash
cd ee/ui-component
npm install
```

### 2. Run

```bash
npm run dev
```
The UI will be available at `http://localhost:3000`.

### 3. Connect to Backend

The frontend requires a connection URI to communicate with the backend.

1.  **Generate a URI:**
    Run this command in a new terminal window:
    ```bash
    curl -X POST http://localhost:8000/local/generate_uri
    ```
    *Note: This assumes your backend is running on port 8000.*

2.  **Enter URI:**
    Paste the generated JSON response (specifically the URI string) into the connection field in the UI.
