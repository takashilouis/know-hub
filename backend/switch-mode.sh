#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 [local|cloud]"
    exit 1
fi

MODE=$1

if [ "$MODE" == "local" ]; then
    echo "Switching to LOCAL mode (Ollama)..."
    cp morphik-local.toml morphik.toml
    echo "Configuration updated. Restarting Morphik..."
    docker restart morphik-core-morphik-1
    echo "Done. Morphik is running in LOCAL mode."
elif [ "$MODE" == "cloud" ]; then
    echo "Switching to CLOUD mode (OpenAI)..."
    # Check if OPENAI_API_KEY is set in .env
    if ! grep -q "OPENAI_API_KEY" .env; then
        echo "WARNING: OPENAI_API_KEY not found in .env."
        echo "Please add it your .env file before using Cloud mode."
        echo "Example: echo 'OPENAI_API_KEY=sk-...' >> .env"
    fi
    cp morphik-cloud.toml morphik.toml
    echo "Configuration updated. Restarting Morphik..."
    docker restart morphik-core-morphik-1
    echo "Done. Morphik is running in CLOUD mode."
else
    echo "Invalid mode. Use 'local' or 'cloud'."
    exit 1
fi
