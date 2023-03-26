from Neo4jConnection import App
from Redis_Connection import Redis
from ReferentialIntegrity import RI
from MetadataModel import MetadataModel
import json


class Services:

    def __init__(self):
        uri = "neo4j+s://a50f760a.databases.neo4j.io:7687"
        user = "neo4j"
        password = "FcvK91QJ62KherRd-cWb1aQFilG0B824UrX87BBcYWQ"
        self.app = App(uri, user, password)

    def find_node_type(self,nodeName):
        node_type = self.app.find_node_type(nodeName)
        self.app.close()
        return node_type

    def find_path_between_nodes(self,nodeName1 , nodeName2):
        node1Type = self.find_node_type(nodeName1)
        node2Type = self.find_node_type(nodeName2)

        path = []
        if(node1Type == 'Entity' and node2Type == 'Entity'):
            path = self.app.return_path_between_entity_to_entity(nodeName1,nodeName2)
        elif (node1Type == 'Attribute' and node2Type == 'Attribute'):
            path = self.app.return_path_between_attribute_to_attribute(nodeName1,nodeName2)
        elif (node1Type == 'Entity' and node2Type == 'Attribute'):
            path = self.app.return_path_between_entity_to_attribute(nodeName1,nodeName2)
        elif (node1Type == 'Attribute' and node2Type == 'Entity'):
            path =  self.app.return_path_between_entity_to_attribute(nodeName2,nodeName1)
        else:
            return None

        nodeNames = []
        nodeTypes = []
        for i in range(0,len(path[0])):
            nodeNames.append(path[0][i].get('name'))
            nodeTypes.append(self.find_node_type(path[0][i].get('name')))

        self.app.close()

        return [nodeNames,nodeTypes]

    def zip_path_between_nodes(self,nodeName1 , nodeName2):

        zipped_list = self.find_path_between_nodes(nodeName1 , nodeName2)
        zipped_list = tuple(zip(zipped_list[0] , zipped_list[1]))

        def _list_to_dict_converter(lst):
            dict_list = []

            for i in lst:
                dct = {
                    "node_name": i[0],
                    "node_type": i[1]
                }

                dict_list.append(dct)
            
            return dict_list

        dct = _list_to_dict_converter(zipped_list)
        return dct


    def print_path_between_nodes(self,list):
        try:
            nodeNames = list[0]
            nodeTypes = list[1]
        except :
            raise Exception("Belirtilen Entity - Attribute Bu Data Modelde bulunmamaktadır.")

        for i in range(0,len(nodeNames)):
            if (i == len(nodeNames)-1):
                print(nodeNames[i]+" ("+nodeTypes[i]+")")
            else:
                print(nodeNames[i]+" ("+nodeTypes[i]+")" + " ----> " , end = '')

    def return_attribute_with_same_datatype(self, nodeType):
        nodes = self.app.return_attribute_with_same_datatype(nodeType)
        nodeNames = []
        for i in range(0,len(nodes[0])):
              dct = {'NodeName': nodes[0][i][0].get('name'),
                    'TableName': nodes[0][i][0].get('tableName'),
                    'DatabaseName': nodes[0][i][0].get('databaseName'),
                    'DataType':nodes[0][i][0].get('dataType')}

              nodeNames.append(dct)
        
        self.app.close()

        return nodeNames

    def return_attribute_with_same_name(self, nodeName):
        nodes = self.app.return_attribute_with_same_name(nodeName)
        nodeNames = []
        for i in range(0,len(nodes[0])):
            
              dct = {'NodeName': nodes[0][i][0].get('name'),
                    'TableName': nodes[0][i][0].get('tableName'),
                    'DatabaseName': nodes[0][i][0].get('databaseName'),
                    'DataType':nodes[0][i][0].get('dataType')}

              nodeNames.append(dct)
        
        return nodeNames

    def redis_query_for_key(self, key):
        rds = Redis()
        value = rds.redis_query(key)

        if value == "DataType":
            data_type = self.return_attribute_with_same_datatype(key)
            return data_type

        elif (value == "Attribute" or value == "Entity"):
            node_name = self.return_attribute_with_same_name(key)
            return node_name

        else:
            raise Exception
    
    def dataframe_to_dictionary(self, df):
            values = df.values
            dct = []
            for item in values:
                dct_str = {
                           "parent_table_name": item[0],
                           "parent_column_name": item[1],
                           "child_table_name": item[2],
                           "child_column_name": item[3],
                           "similarity_point": item[4],
                           "advice": item[6]
                           }
                dct.append(dct_str)
            
            return dct

    
    def referantial_integrity_check_for_potential(self, database_name):
        ri = RI(database_name)
        potential_relations_df = ri.find_system_improvements_potential_relations()
        
        potential_relations_dct = self.dataframe_to_dictionary(potential_relations_df)
        return potential_relations_dct
    

    def referantial_integrity_check_for_existing(self, database_name):
        ri = RI(database_name)
        existing_relations_df = ri.find_system_improvements_existing_relations()
        
        existing_relations_dct = self.dataframe_to_dictionary(existing_relations_df)
        return existing_relations_dct
    
    def get_metadata_model(self, metadata_model_name):
        metadataModel = MetadataModel(metadata_model_name)
        metadataModel.printDatabase()

    def get_all_nodes_infos(self):
        entity_names = self.app.get_all_entity_names()
        id = 0 
        nodes = []
        links = []
        entity_node_numbers = []
        attribute_node_numbers = []

        for entity in entity_names:
            attributes_of_entity = self.app.get_attributes_of_entity(entity)
            entity_id = id
            entity_node_numbers.append(entity_id)
            entity_dct = {
                "node_name": entity, 
                 "node_color": "#428BCA", 
                 "node_size": "15", 
                 "node_size_edge": "0.10000000000000009", 
                 "node_color_edge": "#000000"
            }
            nodes.append(entity_dct)
            id += 1

            for attribute in attributes_of_entity:
                att_dct = {
                    "node_name": attribute, 
                    "node_color": "#FBBC04", 
                    "node_size": "12", 
                    "node_size_edge": "0.10000000000000009", 
                    "node_color_edge": "#000000"
                }
                nodes.append(att_dct)

                links_dct = {
                    "weight":3.28, 
                     "edge_weight": 3.28, 
                     "edge_width": 2.537769784172662, 
                     "source_label": attribute, 
                     "target_label": entity, 
                     "source": id, 
                     "target": entity_id
                    }
                attribute_node_numbers.append(id)
                id +=1
                links.append(links_dct)
        
        entity_repository_object = {
                "node_name": "Entity Repository", 
                 "node_color": "#2B4EFF", 
                 "node_size": "20", 
                 "node_size_edge": "0.10000000000000009", 
                 "node_color_edge": "#000000"
            }
        nodes.append(entity_repository_object)


        for entity_node_number in entity_node_numbers:
                links_dct = {
                    "weight":3.28, 
                     "edge_weight": 3.28, 
                     "edge_width": 2.537769784172662, 
                     "source_label": attribute, 
                     "target_label": entity, 
                     "source": id, 
                     "target": entity_node_number
                    }
                
                links.append(links_dct)
                
        id += 1

        # attribute_repository_object = {
        #         "node_name": "Attribute Repository", 
        #          "node_color": "#FF0000", 
        #          "node_size": "20", 
        #          "node_size_edge": "0.10000000000000009", 
        #          "node_color_edge": "#000000"
        #     }
        # nodes.append(attribute_repository_object)

        # for attribute_node_number in attribute_node_numbers:
        #         links_dct = {
        #             "weight":3.28, 
        #              "edge_weight": 3.28, 
        #              "edge_width": 2.537769784172662, 
        #              "source_label": attribute, 
        #              "target_label": entity, 
        #              "source": id, 
        #              "target": attribute_node_number
        #             }
                
        #         links.append(links_dct)
                
        # id += 1


        graph = {
            "links":links,
            "nodes":nodes
        }


        return json.dumps(graph)
    

        
if __name__ == "__main__":
    service = Services()
    test = service.get_all_nodes_infos()
    print(test)




