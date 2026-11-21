"""Call the NDN or Python gRPC runtime in the Function container."""
import asyncio

import grpc
import function_pb2_grpc
from ndnc.grpc import interpreter_pb2, interpreter_pb2_grpc


class FunctionRuntimeClient:
    def __init__(self, runtime, port):
        self.runtime = runtime
        self.channel = grpc.insecure_channel(
            f'localhost:{port}', options=[('grpc.max_receive_message_length', -1),
                                         ('grpc.max_send_message_length', -1)])
        if runtime == 'ndn':
            self.stub = interpreter_pb2_grpc.InterpreterStub(self.channel)
        else:
            self.stub = function_pb2_grpc.FunctionRuntimeStub(self.channel)

    async def wait_until_ready(self):
        await asyncio.to_thread(grpc.channel_ready_future(self.channel).result, timeout=120)

    def execute(self, request, metadata):
        if self.runtime == 'ndn':
            if request.HasField('binary_input'):
                raise ValueError('Binary arguments require the Python runtime')
            response = self.stub.Execute(
                interpreter_pb2.ExecutionRequest(args=request.args), timeout=20, metadata=metadata)
            if response.WhichOneof('value') == 'binary':
                return response.binary, 'image_ref'
            return response.text.encode('utf-8'), 'result_ref'

        response = self.stub.Execute(request, timeout=20, metadata=metadata)
        if response.HasField('binary_result'):
            return response.binary_result, 'image_ref'
        return response.result.encode('utf-8'), 'result_ref'

    def close(self):
        self.channel.close()
