# interpreter_server.py (新しく作成するgRPCサーバープロセス)
import sys
import grpc
from concurrent import futures
import function_pb2
import function_pb2_grpc
# import importlib.util

class FunctionRuntimeServicer(function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self):
        self.target_function = None

    def DeployFunction(self, request, context):
        code_string = request.code_content
        local_namespace = {}
        try:
            exec(code_string, globals(), local_namespace)
            self.target_function = local_namespace.get('handle')
            if not self.target_function:
                return function_pb2.DeployResponse(success=False, message="No 'handle' function found in the provided code.")
            return function_pb2.DeployResponse(success=True, message="Function deployed successfully.")
        except Exception as e:
            return function_pb2.DeployResponse(success=False, message=str(e))

    def ExecuteFunction(self, request, context):
        if not self.target_function:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("No function deployed.")
            return function_pb2.FunctionResponse(result="")

        args_list = list(request.args)
        result_str = self.target_function(args_list)
        return function_pb2.FunctionResponse(result=result_str)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    function_pb2_grpc.add_FunctionRuntimeServicer_to_server(FunctionRuntimeServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    print(f"Interpreter server is running on port {port}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # ポート番号を引数で受け取る
    serve(sys.argv[1])