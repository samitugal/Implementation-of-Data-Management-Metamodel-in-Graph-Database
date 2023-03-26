from django import forms

class find_path_algorithm(forms.Form):
    sourceNode = forms.CharField(label= 'Source Node' , max_length= 100)
    destinationNode = forms.CharField(label= 'Destination Node' , max_length= 100)


class redis_service_keyword_algorithm(forms.Form):
    node_name = forms.CharField(label = 'Node Name' , max_length = 100)

    
class referential_integrity_service_keyword_algorithm(forms.Form):
    database_name = forms.CharField(label = 'Database Name' , max_length = 100)

class metadata_model_algorithm(forms.Form):
    metadata_model_name = forms.CharField(label = 'Database Name' , max_length = 100)