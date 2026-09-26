"""gRPC client and server bindings for grpc/function.proto."""
import grpc

import function_pb2 as function__pb2


class FunctionRuntimeStub:
    def __init__(self, channel):
        self.ExecuteFunction = channel.unary_unary(
            '/function.FunctionRuntime/ExecuteFunction',
            request_serializer=function__pb2.FunctionRequest.SerializeToString,
            response_deserializer=function__pb2.FunctionResponse.FromString,
            _registered_method=True)


class FunctionRuntimeServicer:
    def ExecuteFunction(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_FunctionRuntimeServicer_to_server(servicer, server):
    rpc_method_handlers = {
        'ExecuteFunction': grpc.unary_unary_rpc_method_handler(
            servicer.ExecuteFunction,
            request_deserializer=function__pb2.FunctionRequest.FromString,
            response_serializer=function__pb2.FunctionResponse.SerializeToString),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        'function.FunctionRuntime', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('function.FunctionRuntime', rpc_method_handlers)
