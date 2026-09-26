#!/bin/bash
set -e

# コンテナ起動時の引数を受け取る
PREFIX=$1
NAMESPACE=$2

PORT=50051
export GRPC_PORT="$PORT"

# ホストからマウントされるNFDソケットのパス
export NDN_CLIENT_TRANSPORT="unix:///run/nfd.sock"

echo "[Container] Starting Interpreter on port $PORT..."
python3 interpreter_server.py "$PORT" &
SERVER_PID=$!

# インタプリタのgRPCサーバーが立ち上がるのを少し待つ
sleep 1

echo "[Container] Deploying function code..."
python3 deploy.py "$PORT" "/app/func.py"

echo "[Container] Starting Sidecar for prefix: $PREFIX"
python3 main.py "$PREFIX" "$NAMESPACE" &
SIDECAR_PID=$!

# コンテナが停止させられた時（docker rm -f など）に、中のプロセスも綺麗に終了させる
trap "echo '[Container] Stopping processes...'; kill -TERM $SERVER_PID $SIDECAR_PID 2>/dev/null; exit" TERM INT

# プロセスを監視してコンテナを起動し続ける
wait $SERVER_PID
wait $SIDECAR_PID