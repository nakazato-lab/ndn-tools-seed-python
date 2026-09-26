FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY sidecar/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/function_pb2.py common/function_pb2_grpc.py ./
COPY sidecar/deploy.py sidecar/main.py sidecar/ndn_function_grpc.py sidecar/ndn_transport.py ./
COPY sidecar/lib ./lib

CMD ["python3", "main.py"]
