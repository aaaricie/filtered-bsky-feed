#!/bin/bash
# ──────────────────────────────────────────────
#  Bluesky Multi-User Feed Generator — Setup
#  Run this ON your Ubuntu server after copying
#  the project files there.
# ──────────────────────────────────────────────
set -e

INSTALL_DIR="$HOME/bsky-feed-multi"
SERVICE_NAME="bsky-feed-multi"

echo "========================================"
echo "  Bluesky Multi-User Feed Generator"
echo "  Setup Script"
echo "========================================"
echo ""

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 not found. Install it with:"
    echo "  sudo apt update && sudo apt install python3"
    exit 1
fi
echo "[ok] Python 3 found: $(python3 --version)"

# Check pip
if ! command -v pip3 &> /dev/null; then
    echo "Installing pip3 ..."
    sudo apt update && sudo apt install -y python3-pip
fi
echo "[ok] pip3 found"

# Create install directory
mkdir -p "$INSTALL_DIR"
echo "[ok] Install directory: $INSTALL_DIR"

# Copy files if running from a different directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ "$SCRIPT_DIR" != "$INSTALL_DIR" ]; then
    cp "$SCRIPT_DIR/feed_server.py" "$INSTALL_DIR/"
    cp "$SCRIPT_DIR/register_feed.py" "$INSTALL_DIR/"
    cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"
    echo "[ok] Copied server files to $INSTALL_DIR"
fi

# Install Python dependencies
echo ""
echo "Installing Python dependencies ..."
pip3 install --user -r "$INSTALL_DIR/requirements.txt"
echo "[ok] Dependencies installed"

# Create .env if it doesn't exist
if [ ! -f "$INSTALL_DIR/.env" ]; then
    if [ -f "$SCRIPT_DIR/.env.example" ]; then
        cp "$SCRIPT_DIR/.env.example" "$INSTALL_DIR/.env"
        echo "[!!] Created .env from example — EDIT IT with your app password:"
        echo "     nano $INSTALL_DIR/.env"
    else
        cat > "$INSTALL_DIR/.env" << 'ENVEOF'
BSKY_HANDLE=your-handle.bsky.social
BSKY_APP_PASSWORD=your-app-password-here
FEED_HOSTNAME=feed.example.com
FEED_PORT=8000
FEED_NAME=filtered-following
DELAY_SECONDS=300
PRUNE_HOURS=48
FOLLOWS_CACHE_TTL=3600
REPOST_HALF_LIFE=6.0
JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe
ENVEOF
        echo "[!!] Created .env — EDIT IT with your app password:"
        echo "     nano $INSTALL_DIR/.env"
    fi
else
    echo "[ok] .env already exists, keeping it"
fi

# Install systemd service
echo ""
echo "Installing systemd service ..."
sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null << SERVICEEOF
[Unit]
Description=Bluesky Multi-User Feed Generator
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=$INSTALL_DIR/.env
ExecStart=/usr/bin/python3 $INSTALL_DIR/feed_server.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICEEOF

sudo systemctl daemon-reload
echo "[ok] Systemd service installed"

echo ""
echo "========================================"
echo "  Setup complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "  1. Edit your .env file with your app password:"
echo "     nano $INSTALL_DIR/.env"
echo ""
echo "  2. Register the feed with Bluesky (one time only):"
echo "     cd $INSTALL_DIR && source .env && export BSKY_APP_PASSWORD BSKY_HANDLE FEED_HOSTNAME FEED_NAME"
echo "     python3 register_feed.py"
echo ""
echo "  3. Start the server:"
echo "     sudo systemctl enable --now $SERVICE_NAME"
echo ""
echo "  4. Check it's running:"
echo "     sudo systemctl status $SERVICE_NAME"
echo "     curl http://localhost:8000/health"
echo ""
echo "  5. View logs:"
echo "     journalctl -u $SERVICE_NAME -f"
echo ""
