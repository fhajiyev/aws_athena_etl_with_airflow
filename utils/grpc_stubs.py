from adrecommender import adrecommender_pb2_grpc
from buzzvil_profile import profile_pb2_grpc


def get_adrecommender_stub():
    return adrecommender_pb2_grpc.AdRecommenderServiceStub


def get_profile_stub():
    return profile_pb2_grpc.ProfileServiceStub
