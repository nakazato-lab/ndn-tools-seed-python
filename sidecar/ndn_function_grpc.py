import asyncio
import os
import random
import subprocess
import datetime
import threading
import logging
from typing import Callable, Optional, List

import grpc
from ndn.app import NDNApp
from ndn.encoding import Name, InterestParam, BinaryStr, FormalName, Component
from ndn.types import InterestNack, InterestTimeout

import function_pb2
import function_pb2_grpc

from lib.ndn_utils import (
    SEGMENT_SIZE,
    extract_first_level_args,
    extract_my_function_name,
    is_function_request,
    get_original_name,
    get_data,
)

logging.basicConfig(format='[{asctime}]{levelname}:{message}',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    style='{')


class NDNFunction:
    def __init__(self):
        logging.info("Initializing NDNFunction")
        self.app = NDNApp()
        self.client_app = NDNApp()
        self.segmented_data = {}
        # client_app をバックグラウンドで起動
        threading.Thread(target=self.client_app.run_forever, daemon=True).start()

        port = os.environ.get('GRPC_PORT', '50051')
        self.grpc_channel = grpc.insecure_channel(f'localhost:{port}')
        self.grpc_stub = function_pb2_grpc.FunctionRuntimeStub(self.grpc_channel)
        
        mobile_code = os.environ.get('MOBILE_CODE')
        if mobile_code:
            logging.info("Deploying mobile code to gRPC server")
            try:
                deploy_req = function_pb2.DeployRequest(code_content=mobile_code)
                deploy_res = self.grpc_stub.DeployFunction(deploy_req)
                if deploy_res.success:
                    logging.info("Mobile code deployed successfully")
                else:
                    logging.error(f"Failed to deploy mobile code: {deploy_res.message}")
                    
            except Exception as e:
                logging.error(f"Error deploying mobile code: {e}")

    def grpc_function_handler(self, name: str, args: List[bytes]) -> bytes:
        logging.info(f"Invoking gRPC function: {name} with args: {[a.decode() for a in args if a is not None]}")
        args_str = [arg.decode() for arg in args if arg is not None]
        request = function_pb2.FunctionRequest(name=name, args=args_str)
        response = self.grpc_stub.ExecuteFunction(request)
        return response.result.encode()

    def run(self, prefix: str, data_request_handler: Callable[[str], str]):
        logging.info(f"Advertising prefix: {prefix}")
        # os.system(f"nlsrc advertise {prefix}")


        @self.app.route(prefix)
        def on_interest(name: FormalName, param: InterestParam, _app_param: Optional[BinaryStr]):
            async def async_on_interest():
                logging.info(f"Received Interest: {Name.to_str(name)} with params: {param}")
                nonce = param.nonce or random.randint(0, 2**32 - 1)
                name_str = Name.to_str(name)
                original_name = get_original_name(name)

                if not is_function_request(original_name):
                    logging.info("Processing as data request")
                    content = data_request_handler(name_str).encode()
                    if Component.get_type(name[-1]) != Component.TYPE_SEGMENT:
                        seg_cnt = (len(content) + SEGMENT_SIZE - 1) // SEGMENT_SIZE
                        packets = [self.app.prepare_data(original_name + [Component.from_segment(i)],
                                                         content[i*SEGMENT_SIZE:(i+1)*SEGMENT_SIZE],
                                                         freshness_period=10000,
                                                         final_block_id=Component.from_segment(seg_cnt - 1))
                                   for i in range(seg_cnt)]
                        self.segmented_data[Name.to_str(original_name)] = packets
                        seg_no = 0
                    else:
                        seg_no = Component.to_number(name[-1])
                    logging.info(f"Sending data segment: {seg_no}")
                    self.app.put_raw_packet(self.segmented_data[Name.to_str(original_name)][seg_no])
                    return

                logging.info("Processing as function request")

                if Component.get_type(name[-1]) != Component.TYPE_SEGMENT:
                    args = extract_first_level_args(name)
                    logging.info(f"Extracted args: {args}")
                    
                    async def fetch(arg: str):
                        for attempt in range(3):
                            logging.info(f"[Fetch] {arg} (try {attempt+1})")
                            try:
                                data = await get_data(
                                    self.client_app, arg,
                                    timeout=1000,   
                                )
                                if data:
                                    logging.info(f"[Fetch] OK {arg}: {data}")
                                    return data
                            except (InterestTimeout, InterestNack) as e:
                                logging.warning(f"[Fetch] {arg} failed: {e}")
                            await asyncio.sleep(0.2)
                        logging.error(f"[Fetch] GIVE‑UP {arg}")
                        return None

                    tasks = [fetch(arg) for arg in args]
                    contents = await asyncio.gather(*tasks)

                    if any(c is None for c in contents):
                        logging.error(f"One or more args failed to fetch. contents: {contents}")
                        return

                    my_function_name = extract_my_function_name(name)
                    logging.info(f"Executing function: {my_function_name}")
                    result = self.grpc_function_handler(my_function_name, contents)
                    logging.info(f"Function result (raw): {result}")

                    seg_cnt = (len(result) + SEGMENT_SIZE - 1) // SEGMENT_SIZE
                    packets = [self.app.prepare_data(original_name + [Component.from_segment(i)],
                                                     result[i*SEGMENT_SIZE:(i+1)*SEGMENT_SIZE],
                                                     freshness_period=10000,
                                                     final_block_id=Component.from_segment(seg_cnt - 1))
                               for i in range(seg_cnt)]
                    logging.debug(f"Segmented packets stored for {Name.to_str(original_name)}: {len(packets)} segments")

                    self.segmented_data[Name.to_str(original_name)] = packets
                    seg_no = 0
                else:
                    seg_no = Component.to_number(name[-1])
                logging.info(f"Sending function response segment: {seg_no}")
                self.app.put_raw_packet(self.segmented_data[Name.to_str(original_name)][seg_no])

            asyncio.create_task(async_on_interest())

        logging.info("Starting NDN event loop")
        self.app.run_forever()
