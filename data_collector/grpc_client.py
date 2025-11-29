# data_collector/grpc_client.py
import os
import grpc
from grpc_definitions import user_pb2, user_pb2_grpc

class UserManagerClient:
    def __init__(self, host=None, port=None):
        host = host or os.getenv("USER_MANAGER_HOST", "user_manager")
        port = int(port or os.getenv("USER_MANAGER_GRPC_PORT", 50051))
        target = f"{host}:{port}"
        self.channel = grpc.insecure_channel(target)
        self.stub = user_pb2_grpc.UserServiceStub(self.channel)

    def user_exists(self, email, timeout=5):
        try:
            req = user_pb2.UserCheckRequest(email=email)
            res = self.stub.CheckUser(req, timeout=timeout)
            return res.exists, None
        except grpc.RpcError as e:
            return False, f"gRPC error: {e.code()} - {e.details() if hasattr(e, 'details') else str(e)}"
    def close(self):
        try:
            self.channel.close()
        except:
            pass
