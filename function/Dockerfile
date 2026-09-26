FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY function/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY grpc/function_pb2.py grpc/function_pb2_grpc.py ./
COPY function/interpreter_server.py function/ndn_runtime.py ./

CMD ["python3", "interpreter_server.py", "50051"]
