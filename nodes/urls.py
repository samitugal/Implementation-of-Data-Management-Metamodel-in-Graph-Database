from django.urls import path
from . import views

# http://127.0.0.1:8000/
# http://127.0.0.1:8000/home
# http://127.0.0.1:8000/find_path
# http://127.0.0.1:8000/find_node_type


urlpatterns = [

    path("" , views.homepage),

    # Find Path Service URL's
    path("find_path_service/", views.find_path_service),
    path("find_path_service/processing/" , views.find_path_processing , name = "find_path_processing"),

    # Redis DataType or Entity URL's
    path("redis_query_service/", views.redis_query_service),
    path("redis_query_service/processing/" , views.redis_query_service_processing , 
    name = "similar_nodes_service"),
    
    # Referetial Integrity URL's
    path("referential_integrity_service/", views.referential_integrity_service),
    path("referential_integrity_service/processing/" , views.referential_integrity_service_processing , 
    name = "referential_integrity_service"),

    # Graph Map URL's
    path("graph_map_service/", views.graph_map_service),
    path("graph_map_service/processing/" , views.graph_map_processing , 
    name = "graph_map_service"),


]