import sys
import grpc
import function_pb2
import function_pb2_grpc

def deploy(port, file_path):
    # マウントされたコードファイルを読み込む
    try:
        with open(file_path, 'r') as f:
            code = f.read()
    except FileNotFoundError:
        print(f"[Deployer] Error: File {file_path} not found.")
        sys.exit(1)

    # gRPCサーバーに接続
    channel = grpc.insecure_channel(f'localhost:{port}')
    
    grpc.channel_ready_future(channel).result(timeout=30)

    # ★修正1: service名に基づき FunctionRuntimeStub を使用
    stub = function_pb2_grpc.FunctionRuntimeStub(channel)

    # ★修正2: protoの定義に従い code_content のみを渡す
    request = function_pb2.DeployRequest(
        code_content=code
    )
    
    try:
        response = stub.DeployFunction(request, timeout=30)
        if response.success:
            print(f"[Deployer] Function deployed successfully!")
        else:
            print(f"[Deployer] Deployment failed: {response.message}")
            sys.exit(1)
    except Exception as e:
        print(f"[Deployer] gRPC connection error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python deploy.py <port> <file_path>")
        sys.exit(1)
    deploy(sys.argv[1], sys.argv[2])