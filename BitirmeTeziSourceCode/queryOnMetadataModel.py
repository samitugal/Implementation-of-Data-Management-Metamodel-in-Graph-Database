from Neo4jConnection import App
from Redis_Connection import Redis
from ReferentialIntegrity import RI


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
    

        
if __name__ == "__main__":
    service = Services()
    test = service.referantial_integrity_check_for_potential("TestDB")


