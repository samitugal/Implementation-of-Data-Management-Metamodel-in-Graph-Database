from django.shortcuts import render
from django.http import JsonResponse
from BitirmeTeziSourceCode.queryOnMetadataModel import Services
from .forms import find_path_algorithm,  redis_service_keyword_algorithm, referential_integrity_service_keyword_algorithm, metadata_model_algorithm

import sys
sys.path.append(".")

# Create your views here.

def homepage(request):
    return render(request, "homepage.html")


#### FIND PATH SERVICE ###
def find_path_service(request):

    algorithm = find_path_algorithm()
    return render(request, "find_path_page.html" , {'algorithm': algorithm})


def find_path_processing(request):

    if request.method == 'POST':
        form = find_path_algorithm()
        service = Services()

        # if form is valid kontrolü gelebilir
        source_node_name = request.POST['sourceNode']
        destination_node_name = request.POST['destinationNode']
        path = service.zip_path_between_nodes(source_node_name , destination_node_name)

    return render(request , "find_path_processing.html", {'path':path})

### REDIS SERVICE

def redis_query_service(request):

    algorithm = redis_service_keyword_algorithm()
    return render(request, "redis_query_service.html" , {'algorithm': algorithm})


def redis_query_service_processing(request):

    if request.method == 'POST':
        form = redis_service_keyword_algorithm()
        service = Services()

        # if form is valid kontrolü gelebilir
        node_name = request.POST['node_name']
        nodes = service.redis_query_for_key(node_name)
        
    return render(request , "redis_query_service_processing.html", {'nodes':nodes})

### REFERENTIAL INTEGRITY SERVICE

def referential_integrity_service(request):

    algorithm = referential_integrity_service_keyword_algorithm()
    return render(request, "referential_integrity_service.html" , {'algorithm': algorithm})


def referential_integrity_service_processing(request):

    if request.method == 'POST':
        form = referential_integrity_service_keyword_algorithm()
        service = Services()

        # if form is valid kontrolü gelebilir
        database_name = request.POST['database_name']
        items = service.referantial_integrity_check_for_potential(database_name)
        
    return render(request , "referential_integrity_service_processing.html", {'items':items})


### GRAPH MAP SERVICE

def graph_map_service(request):

    algorithm = referential_integrity_service_keyword_algorithm()
    return render(request, "graph_map_service.html" , {'algorithm': algorithm})


def graph_map_processing(request):

    if request.method == 'POST':
        form = metadata_model_algorithm()
        service = Services()

        # if form is valid kontrolü gelebilir
        database_name = request.POST['database_name']
        service.get_metadata_model(database_name)

        items = service.get_all_nodes_infos()
        
    return render(request , "graph_map_processing.html", {'items':items})




























