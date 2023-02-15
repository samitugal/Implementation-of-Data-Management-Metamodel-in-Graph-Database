
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

#%% Database Object
    def create_database(self, database_name, tableCount,create_date):
        with self.driver.session(database="neo4j") as session:
            result = session.write_transaction(
                self._create_and_return_database, database_name, tableCount , create_date)
            for row in result:
                print("New Data Model Object Created: {db}".format(db=row['db']))

    @staticmethod
    def _create_and_return_database(tx, database_name , tableCount, createDate):
        query = (
            "CREATE (db:DataModel { name: $database_name, table_count: $tableCount, create_date: $createDate}) "
            "RETURN db"
        )
        result = tx.run(query, database_name=database_name , tableCount = tableCount, createDate = createDate)
        try:
            return [{"db": row["db"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    
#%% Table Object

    def create_table(self, table_name, database_name,create_date,modify_date,primaryKey,foreignKey,uniqueKey,primaryKeyName):
        with self.driver.session(database="neo4j") as session:
            result = session.write_transaction(
                self._create_and_return_table, table_name, database_name , create_date, modify_date,primaryKey,foreignKey,uniqueKey,primaryKeyName)
            for row in result:
                print("New Entity Object Created: {tb}".format(tb=row['tb']))

    
    @staticmethod
    def _create_and_return_table(tx, tableName, databaseName , createDate, modifyDate, primaryKey,foreignKey,uniqueKey,primaryKeyName):
        query = (
            "CREATE (tb:Entity {name: $tableName," 
            " databaseName: $databaseName,"
            " create_date: $createDate," 
            " modify_date: $modifyDate, " 
            " primary_key:$primaryKey, " 
            " foreign_key:$foreignKey,"
            " unique_key:$uniqueKey," 
            " primary_key_name: $primaryKeyName}) "                               
            " RETURN tb"
        )
        result = tx.run(query, tableName = tableName, 
                        databaseName=databaseName, 
                        createDate = createDate, 
                        modifyDate = modifyDate, 
                        primaryKey = primaryKey, 
                        foreignKey = foreignKey , 
                        uniqueKey = uniqueKey,
                        primaryKeyName = primaryKeyName )
        try:
            return [{"tb": row["tb"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
#%% Create Relations Between Database And Tables
    
    def create_relations_between_database_table(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relations_between_database_table,database_name)
            print("Relations Between Data Model and Entities Set")
          
    @staticmethod
    def _create_relations_between_database_table(tx,databaseName):
        query = (
            "MATCH (db:DataModel),(tb:Entity) WHERE db.name CONTAINS tb.databaseName AND db.name = $databaseName CREATE  (tb)-[:PARTS_OF]-> (db) RETURN db,tb"
        )
        tx.run(query,databaseName = databaseName)

#%% Create Center Point and Create Relations Between Database and Center
   
    def create_center_point(self):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_center_point)
            print("Center Point Set")
          
    @staticmethod
    def _create_center_point(tx):
        query = (
            "CREATE (ct:MDR { name: 'Metadata Repository'})"
        )
        tx.run(query)
        
    def create_relation_between_database_center(self):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relation_between_database_center)
            print("Relations Between MDR and Data Models Set")
          
    @staticmethod
    def _create_relation_between_database_center(tx):
        query = ("match(n:DataModelRepository),(m:MDR) CREATE (m)-[:CONTAINS]->(n)")
        query1 = ("match(e:EntityRepository),(m:MDR) CREATE (m)-[:CONTAINS]->(e)")
        query2 = ("match(a:AttributeRepository),(m:MDR) CREATE (m)-[:CONTAINS]->(a)")
        tx.run(query)
        tx.run(query1)
        tx.run(query2)
    
#%% Create Column Object

    def create_column(self, column_name, table_name, database_name, data_type, is_nullable):
        with self.driver.session(database="neo4j") as session:
            result = session.write_transaction(
                self._create_and_return_column,column_name, table_name, database_name, data_type, is_nullable)
            for row in result:
                print("New Attribute Object Created: {cl}".format(cl=row['cl']))

    
    @staticmethod
    def _create_and_return_column(tx, columnName, tableName, databaseName, dataType, isNullable):
        query = (
            "CREATE (cl:Attribute {name: $columnName, tableName: $tableName, databaseName: $databaseName, dataType: $dataType, isNullable: $isNullable }) "
            "RETURN cl"
        )
        result = tx.run(query, columnName = columnName, tableName=tableName, databaseName = databaseName, dataType = dataType, isNullable= isNullable )
        try:
            return [{"cl": row["cl"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

#%% Create Relations Between Table And Column
    
    def create_relations_between_table_columns(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relations_between_table_column, database_name)
            print("Relations Between Entity and Attributes Set")
          
    @staticmethod
    def _create_relations_between_table_column(tx,databaseName):
        query = (
            "MATCH (tb:Entity),(cl:Attribute) WHERE tb.name = cl.tableName AND cl.databaseName = $databaseName AND cl.databaseName = tb.databaseName  CREATE  (cl)-[:COLUMNS_OF]-> (tb) RETURN cl,tb"
        )
        tx.run(query, databaseName = databaseName)
            
#%% View Object

    def create_view(self, view_name, database_name,create_date,modify_date):
        with self.driver.session(database="neo4j") as session:
            result = session.write_transaction(
                self._create_and_return_view, view_name, database_name , create_date, modify_date)
            for row in result:
                print("New View Object Created: {vw}".format(vw=row['vw']))

    
    @staticmethod
    def _create_and_return_view(tx, viewName, databaseName , createDate, modifyDate):
        query = (
            "CREATE (vw:View {name: $viewName, databaseName: $databaseName, create_date: $createDate, modify_date: $modifyDate}) "
            "RETURN vw"
        )
        result = tx.run(query, viewName = viewName, databaseName=databaseName, createDate = createDate, modifyDate = modifyDate)
        try:
            return [{"vw": row["vw"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
#%% Create Relations Between Database And Tables
    
    def create_relations_between_database_view(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relations_between_database_view,database_name)
            print("Relations Between Data Model and Views Set")
          
    @staticmethod
    def _create_relations_between_database_view(tx,databaseName):
        query = (
            "MATCH (db:DataModel),(vw:View) WHERE db.name = vw.databaseName AND db.name = $databaseName CREATE  (vw)-[:PARTS_OF]-> (db) RETURN db,vw"
        )
        tx.run(query,databaseName = databaseName)
    
#%% Create Relation Between Similar Table Names
    def create_relation_between_similar_table_names(self,db1_name,tb1_name,db2_name,tb2_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relation_between_similar_table_names,db1_name,tb1_name,db2_name,tb2_name)
            print("Created Relation Between Similar Tables")
          
    @staticmethod
    def _create_relation_between_similar_table_names(tx,db1_name,tb1_name,db2_name,tb2_name):
        query = (
            " match (tb1:Entity{name:$tb1_name, databaseName:$db1_name}),(tb2:Entity{name:$tb2_name, databaseName:$db2_name})"
            " WHERE (tb1.databaseName <> tb2.databaseName AND tb1.name <> tb2.name)"
            " CREATE (tb1)-[:SIMILAR_WITH]->(tb2)"
            " RETURN tb1,tb2"
        )
        tx.run(query,db1_name=db1_name,tb1_name=tb1_name,db2_name=db2_name,tb2_name=tb2_name)
        
        
#%% Create Relation Between Foreign Key Colunms
    def create_foreign_key_relation(self,database_name,parent_table_name,parent_column_name,child_table_name,child_column_name,foreignKeyName):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_foreign_key_relation,database_name,parent_table_name,parent_column_name,child_table_name,child_column_name,foreignKeyName)        
            print("Created Foreign Key Relation")
    @staticmethod
    def _create_foreign_key_relation(tx,database_name,parent_table_name,parent_column_name,child_table_name,child_column_name,foreignKeyName):
        query = (
               " MATCH (cl1:Attribute),(cl2:Attribute),(ent1:Entity{name:cl1.tableName}),(ent2:Entity{name:cl2.tableName}) "
               " where cl1.databaseName = cl2.databaseName "
               " AND cl1.databaseName = $database_name "
               " AND cl1.tableName = $parent_table_name "
               " AND cl1.name = $parent_column_name " 
               " AND cl2.tableName = $child_table_name "
               " AND cl2.name = $child_column_name "
               " CREATE (ent2)-[rf:REFERS_TO{name: $foreignKeyName}]->(ent1) "
               " RETURN cl1,cl2"
        )
        
        query1 = (
               " MATCH (cl1:Attribute),(cl2:Attribute),(ent1:Entity{name:cl1.tableName}),(ent2:Entity{name:cl2.tableName}) "
               " where cl1.databaseName = cl2.databaseName "
               " AND cl1.databaseName = $database_name "
               " AND cl1.tableName = $parent_table_name "
               " AND cl1.name = $parent_column_name " 
               " AND cl2.tableName = $child_table_name "
               " AND cl2.name = $child_column_name "
               " CREATE (cl1)-[rf:RELATED_WITH{name: $foreignKeyName}]->(cl2) "
               " RETURN cl1,cl2"
        )
        tx.run(query,database_name=database_name,
               parent_table_name=parent_table_name,
               parent_column_name=parent_column_name,
               child_table_name=child_table_name,
               child_column_name=child_column_name,
               foreignKeyName = foreignKeyName)

        tx.run(query1,database_name=database_name,
               parent_table_name=parent_table_name,
               parent_column_name=parent_column_name,
               child_table_name=child_table_name,
               child_column_name=child_column_name,
               foreignKeyName = foreignKeyName)          


#%% Create Entity Object and Create Relations Between Entity and Tables
   
    def create_entity_object(self):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_entity_object)
            print("Entity Repository Object Set")
          
    @staticmethod
    def _create_entity_object(tx):
        query = (
            "CREATE (ent:EntityRepository { name: 'Entity Repository'})"
        )
        tx.run(query)
        
    def create_relation_between_tables_entity(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relation_between_tables_entity, database_name)
            print("Relations Between Entity Object and Tables Set")
          
    @staticmethod
    def _create_relation_between_tables_entity(tx, databaseName):
        query = (
            "match(tb:Entity),(ent:EntityRepository) WHERE tb.databaseName = $databaseName CREATE (tb)-[:INSTANCES_OF]->(ent)"
        )
        tx.run(query, databaseName = databaseName)

#%% Create Attribute Object and Create Relations Between Attribute and Columns
   
    def create_attribute_object(self):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_attribute_object)
            print("Attribute Repository Object Set")
          
    @staticmethod
    def _create_attribute_object(tx):
        query = (
            "CREATE (att:AttributeRepository { name: 'Attribute Repository'})"
        )
        tx.run(query)
        
    def create_relation_between_columns_attribute(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relation_between_columns_attribute, database_name)
            print("Relations Between Attribute Object and Columns Set")
          
    @staticmethod
    def _create_relation_between_columns_attribute(tx, databaseName):
        query = (
            "match(cl:Attribute),(att:AttributeRepository) WHERE cl.databaseName = $databaseName CREATE (cl)-[:INSTANCES_OF]->(att)"
        )
        tx.run(query, databaseName = databaseName)


#%% Find Node Type

    def find_node_type(self, node_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_node_type, node_name)
            return result
        
    @staticmethod
    def _find_node_type(tx, node_name):
        query = (
            " MATCH (p) "
            " WHERE p.name = $node_name"
            " RETURN labels(p)"

        )
        result = tx.run(query, node_name=node_name)
        nodeType = ''
        for row in result:
            nodeType = row[0][0]

        return nodeType
    
#%% Delete Old Metadata Model

    def delete_old_metadata_model(self):
        with self.driver.session(database="neo4j") as session:
            session.execute_read(self._delete_old_metadata_model)
            print("Old Metadata Model Deleted")
        
    @staticmethod
    def _delete_old_metadata_model(tx):
        query = (
            " MATCH (p) DETACH DELETE p "
        )
        tx.run(query)
        
#%% Find Path Between Entity-to-Entity

    def return_path_between_entity_to_entity(self, node_name1 , node_name2):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._return_path_between_entity_to_entity, node_name1,node_name2)
            return result
        
    @staticmethod
    def _return_path_between_entity_to_entity(tx, node_name1,node_name2):
        query = (
                " MATCH (ent:Entity {name: $node_name1} ),"
                       " (ent1:Entity {name: $node_name2}),"
                     " p = shortestPath((ent)-[:REFERS_TO| COLUMNS_OF*]-(ent1))"
                " WHERE length(p) > 1"
                " RETURN nodes(p)"
        )
        result = tx.run(query, node_name1 = node_name1 , node_name2 = node_name2)
        
        for row in result: return row

#%% Find Path Between Entity-to-Attribute

    def return_path_between_entity_to_attribute(self, node_name1 , node_name2):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._return_path_between_entity_to_attribute, node_name1,node_name2)
            return result
        
    @staticmethod
    def _return_path_between_entity_to_attribute(tx, node_name1,node_name2):
        query = (
                " MATCH (ent:Entity {name: $node_name1} ),"
                       " (ent1:Attribute {name: $node_name2}),"
                     " p = shortestPath((ent)-[:REFERS_TO | COLUMNS_OF*]-(ent1))"
                " WHERE length(p) > 1"
                " RETURN nodes(p)"
        )
        result = tx.run(query, node_name1 = node_name1 , node_name2 = node_name2)
        
        for row in result: return row

#%% Find Path Between Attribute-to-Attribute

    def return_path_between_attribute_to_attribute(self, node_name1 , node_name2):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._return_path_between_attribute_to_attribute, node_name1,node_name2)
            return result
        
    @staticmethod
    def _return_path_between_attribute_to_attribute(tx, node_name1,node_name2):
        query = (
            
                " MATCH (ent:Attribute {name: $node_name1} ),"
                       " (ent1:Attribute {name: $node_name2}),"
                    " p = shortestPath((ent)-[:REFERS_TO | COLUMNS_OF | SIMILAR_WITH*]-(ent1))"
                 " WHERE length(p) > 1"
                 " RETURN nodes(p)"
        )
        result = tx.run(query, node_name1 = node_name1 , node_name2 = node_name2)
        
        for row in result: return row

#%% Find Node's Data Type

    def find_node_data_type(self, node_name, node_entity_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_node_data_type, node_name, node_entity_name)
            return result
        
    @staticmethod
    def _find_node_data_type(tx, node_name,node_entity_name):
        query = (
            " Match (n:Attribute{name: $node_name, tableName: $node_entity_name}) return properties(n).dataType "

        )
        result = tx.run(query, node_name=node_name, node_entity_name = node_entity_name)
        nodeType = ''
        for row in result:

            nodeType = row[0]
            
        return nodeType

#%% Setting Primary Keys

    def set_primary_key_nodes(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.execute_read(self._set_primary_key_nodes, database_name)
            print("Primary Key Node Types Set")
        
    @staticmethod
    def _set_primary_key_nodes(tx,database_name):
        query = (
            " MATCH (p:Attribute)-[:COLUMNS_OF]->(ent:Entity) "
            " WHERE ent.primary_key_name = p.name "
            " AND ent.databaseName = $database_name"
            " WITH DISTINCT p "
            " SET p:Primary_Key "
        )
        tx.run(query, database_name = database_name)

#%% Create Data Model Object and Create Relations Between Data Model Repository and Data Models
   
    def create_datamodel_object(self):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_datamodel_object)
            print("Data Model Repository Object Set")
          
    @staticmethod
    def _create_datamodel_object(tx):
        query = (
            "CREATE (att:DataModelRepository { name: 'Data Model Repository'})"
        )
        tx.run(query)
        
    def create_relation_between_datamodel_datamodelrepo(self, database_name):
        with self.driver.session(database="neo4j") as session:
            session.read_transaction(self._create_relation_between_datamodel_datamodelrepo, database_name)
            print("Relations Between Data Model Repository and Data Model Set")
          
    @staticmethod
    def _create_relation_between_datamodel_datamodelrepo(tx, databaseName):
        query = (
            "match(cl:DataModel),(att:DataModelRepository) WHERE cl.name = $databaseName CREATE (cl)-[:INSTANCES_OF]->(att)"
        )
        tx.run(query, databaseName = databaseName)


#%% Check Relation Between Entities
     
    def check_relation_between_attributes(self, att_name1 , att_name2,table_name1, table_name2):
        with self.driver.session(database="neo4j") as session:
            result = session.read_transaction(self._check_relation_between_attributes,att_name1 ,att_name2,
                                              table_name1, table_name2)
            return result
          
    @staticmethod
    def _check_relation_between_attributes(tx, att_name1 , att_name2, table_name1, table_name2):
        query = (
            "match (n:Attribute{name:$att_name1,tableName:$table_name1 })-[ref:RELATED_WITH]-(m:Attribute{name:$att_name2 ,tableName:$table_name2}) return ref"
        )
        result = tx.run(query, att_name1 = att_name1 , att_name2 = att_name2, 
                        table_name1 = table_name1, table_name2 = table_name2)
        for row in result: return row

#%% Check Is Attribute Primary Key
     
    def check_is_it_primary_key(self, attribute_name, entity_name):
        with self.driver.session(database="neo4j") as session:
            result = session.read_transaction(self._check_is_it_primary_key,attribute_name, entity_name)
            return result
          
    @staticmethod
    def _check_is_it_primary_key(tx, attribute_name, entity_name):
        query = (
            " MATCH (p:Attribute),(n:Entity) "
            " WHERE p.name = $attribute_name"
            " and n.name = $entity_name"
            " AND p.tableName = n.name"
            " AND n.primary_key_name = p.name"
            " return p"

        )
        result = tx.run(query, attribute_name = attribute_name, entity_name = entity_name)
        for row in result: return row


#%% Return Attributes with Same Data Type

    def return_attribute_with_same_datatype(self, data_type):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._return_attribute_with_same_datatype, data_type)
            return result
        
    @staticmethod
    def _return_attribute_with_same_datatype(tx , data_type):
        query = 'MATCH p = (a{dataType: $data_type}) RETURN COLLECT(nodes(p))'

        result = tx.run(query , data_type = data_type)
        
        for row in result: return row
    
#%% Return Attributes with Same Name

    def return_attribute_with_same_name(self, data_type):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._return_attribute_with_same_name, data_type)
            return result
        
    @staticmethod
    def _return_attribute_with_same_name(tx , data_type):
        query = 'MATCH p = (a:Attribute{name: $data_type}) RETURN COLLECT(nodes(p))'

        result = tx.run(query , data_type = data_type)
        
        for row in result: return row
        
#%% Get All Entity Names And Their Types
    
    def get_all_entity_names_and_types(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._get_all_entity_names_and_types)
            return result
        
    @staticmethod
    def _get_all_entity_names_and_types(tx):
        query = 'MATCH (a:Entity) RETURN COLLECT(a.name)'

        result = tx.run(query)
        
        for row in result: return row[0]

#%% Get All Attribute Names And Their Types
    
    def get_all_attribute_names_and_types(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._get_all_attribute_names_and_types)
            return result
        
    @staticmethod
    def _get_all_attribute_names_and_types(tx):
        query = 'MATCH (a:Attribute) RETURN COLLECT(a.name)'

        result = tx.run(query)
        
        for row in result: return row[0]























    
    
    
    
    
    
    
    

