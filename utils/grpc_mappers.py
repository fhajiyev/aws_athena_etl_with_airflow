from adrecommender import adrecommender_pb2
from buzzvil_profile import profile_pb2
from pandas import DataFrame


def transform_to_adrecommendersvc_create_ad_embedding_requests(rows):
    requests = []

    for row in rows:
        creatives = []

        names = row[1].split(',')
        urls = row[2].split(',')

        for i, url in enumerate(urls):
            creatives.append(adrecommender_pb2.Creative(image_url=url, name=names[i]))
        requests.append(adrecommender_pb2.CreateAdEmbeddingRequest(ad_id=row[0], creatives=creatives))
    return requests


def transform_to_list_profile_ids_requests(df_profile_dimensions: DataFrame):
    return profile_pb2.ListProfileIDsRequest(
        profiles=[
            profile_pb2.Profile(
                ifa=dim_row.get('ifa'),
                app_id=dim_row.get('app_id'),
                publisher_user_id=dim_row.get('publisher_user_id'),
                cookie_id=dim_row.get('cookie_id'),
                account_id=dim_row.get('account_id')
            ) for index, dim_row in df_profile_dimensions.iterrows()
        ]
    )
