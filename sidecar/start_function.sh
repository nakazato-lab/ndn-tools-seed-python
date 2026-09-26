#!/bin/bash

cd "$(dirname "$0")"

# 引数の受け取り
PREFIX=$1        # 例: /localhost/demo/seed/funcA
NAMESPACE=$2     # 例: namespace情報
PORT=$3          # 例: 50051 (動的割り当てポート)

export NDN_CLIENT_TRANSPORT="unix:///run/nfd/nfd.sock"
export GRPC_PORT="$PORT"

echo "[start_function.sh] Starting function: $PREFIX on port $PORT"

# 1. インタプリタプロセス（gRPCサーバー）をバックグラウンド起動
python3 interpreter_server.py "$PORT" "$CODE_FILE" &
SERVER_PID=$!

sleep 1

# 2. サイドカープロセス（NDNクライアント）をバックグラウンド起動
# 既存の main.py の引数を変えずに済むよう、ポート番号は環境変数で渡します
python3 main.py "$PREFIX" "$NAMESPACE" &
SIDECAR_PID=$!

# 3. シグナルのトラップ（超重要）
# seed.cpp が DELETE 命令で kill() を送ってきた際、このスクリプトの子プロセスも一緒に終了させます
trap "echo '[start_function.sh] Stopping...'; kill -TERM $SERVER_PID $SIDECAR_PID 2>/dev/null; exit" TERM INT

# 4. バックグラウンドプロセスが終了するまで待機（スクリプトを終了させない）
wait $SERVER_PID
wait $SIDECAR_PID
