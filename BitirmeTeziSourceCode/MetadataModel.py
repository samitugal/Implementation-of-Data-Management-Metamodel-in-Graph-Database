import pandas as pd
from databaseAnalysis import DB
from Neo4jConnection import App
from Redis_Connection import Redis

class MetadataModel:
    
    def __init__(self,DATABASE_NAME):
        self.DATABASE_NAME = DATABASE_NAME
        self.DATABASE_CONN = DB('SQL SERVER','DESKTOP-7P8DAD8',DATABASE_NAME)
        uri = "neo4j+s://a50f760a.databases.neo4j.io:7687"
        user = "neo4j"
        password = "FcvK91QJ62KherRd-cWb1aQFilG0B824UrX87BBcYWQ"      
        self.NEO4J_CONN = App(uri, user, password)
        self.REDIS_CONN = Redis()
    
    def createMetadataModel(self):
             
        def readDataFromDB(self,Query):
            df = pd.read_sql_query(Query,self.DATABASE_CONN.CONNECTION)
            return df
          
        def createDatabaseObject(self):
                           
            tableCount = self.DATABASE_CONN.check_table_count_in_database()
            createDate = self.DATABASE_CONN.check_table_create_date_database()
         
            self.NEO4J_CONN.create_database(str(self.DATABASE_NAME),str(tableCount),str(createDate))
            self.NEO4J_CONN.close()
        
        def createTables(self): 
            tables = self.DATABASE_CONN.check_table_properties()
            
            for i in range(0,len(tables)):
                
                primary_key = self.DATABASE_CONN.find_primary_key_of_table(str(tables[i][1]))
                if(len(primary_key) != 0): primary_key = primary_key[0][0]
                else: primary_key = "-"
                
                self.NEO4J_CONN.create_table(str(tables[i][1]), str(tables[i][0]), 
                                 str(tables[i][2]), str(tables[i][3]), 
                                 str(tables[i][5]), str(tables[i][4]),
                                 str(tables[i][6]), str(primary_key))
              
        def createViews(self):
            views = self.DATABASE_CONN.check_database_views()
            for i in range(0,len(views)):             
                self.NEO4J_CONN.create_view(str(views[i][1]),
                                str(views[i][0]),
                                str(views[i][2]),
                                str(views[i][3]))  
                    
        def createColumns(self):           
            tables = self.DATABASE_CONN.check_only_table_names_in_database()  
                
            columns = []
            for i in tables:                   
                tmp = self.DATABASE_CONN.check_columns_of_table(str(i[0]))
                columns.append(tmp)
                
            for i in range(0,len(columns)):
                for j in range(0,len(columns[i])):
                    self.NEO4J_CONN.create_column(str(columns[i][j][2]),
                                      str(columns[i][j][1]),
                                      str(columns[i][j][0]),
                                      str(columns[i][j][3]),
                                      str(columns[i][j][4]))
        
        def createForeignKeyRelations(self):
            foreignKeyList = self.DATABASE_CONN.findForeignKeyRelations()
            for i in foreignKeyList:
                self.NEO4J_CONN.create_foreign_key_relation(str(i[0]),
                                                str(i[1]),
                                                str(i[2]),
                                                str(i[3]),
                                                str(i[4]),
                                                str(i[5]))
        
        def create_relation_between_similar_table_names(self):
            similar_table_names = self.DATABASE_CONN.find_similarities_between_tables()
            for i in similar_table_names:
                self.NEO4J_CONN.create_relation_between_similar_table_names(str(i[0]),
                                                                str(i[1]),
                                                                str(i[2]),
                                                                str(i[3]))
        

            
        
        createDatabaseObject(self)
        createTables(self)
        createViews(self)
        self.NEO4J_CONN.create_relations_between_database_table(self.DATABASE_NAME)
        self.NEO4J_CONN.create_relations_between_database_view(self.DATABASE_NAME)
        createColumns(self)
        self.NEO4J_CONN.create_relations_between_table_columns(self.DATABASE_NAME)
        self.NEO4J_CONN.create_relation_between_tables_entity(self.DATABASE_NAME)
        self.NEO4J_CONN.create_relation_between_columns_attribute(self.DATABASE_NAME)
        self.NEO4J_CONN.set_primary_key_nodes(self.DATABASE_NAME)
        self.NEO4J_CONN.create_relation_between_datamodel_datamodelrepo(self.DATABASE_NAME)
        createForeignKeyRelations(self)

        def insert_into_redis():
            
            entity_list = self.NEO4J_CONN.get_all_entity_names_and_types()
            attribute_list = self.NEO4J_CONN.get_all_attribute_names_and_types()
            data_type_list = self.DATABASE_CONN.get_data_types_in_database()
            
            for attribute_node in attribute_list:
                self.REDIS_CONN.redis_insertion(attribute_node, "Attribute")
                
            for entity_node in entity_list:
                self.REDIS_CONN.redis_insertion(entity_node, "Entity")
                
            for data_type in data_type_list:

                self.REDIS_CONN.redis_insertion(data_type, "DataType")
        
        
        insert_into_redis()
        
        
    def printAllDatabases(self):
        databaseNames = self.DATABASE_CONN.searchDatabases()
        
        self.REDIS_CONN.flushall()
        self.NEO4J_CONN.delete_old_metadata_model()
        self.NEO4J_CONN.create_center_point()
        self.NEO4J_CONN.create_entity_object()
        self.NEO4J_CONN.create_attribute_object()
        
        for i in databaseNames:
            metadatamodel = MetadataModel(i)
            metadatamodel.createMetadataModel()
            
        self.NEO4J_CONN.create_relation_between_database_center()
              
    def printDatabase(self):
       
        self.REDIS_CONN.flushall()
        self.NEO4J_CONN.delete_old_metadata_model()
        self.NEO4J_CONN.create_center_point()
        self.NEO4J_CONN.create_entity_object()
        self.NEO4J_CONN.create_attribute_object()
        self.NEO4J_CONN.create_datamodel_object()
        
        self.createMetadataModel()
        self.NEO4J_CONN.create_relation_between_database_center()


                             
if __name__ == "__main__":
    metadataModel = MetadataModel("Northwind")
    metadataModel.printDatabase()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    