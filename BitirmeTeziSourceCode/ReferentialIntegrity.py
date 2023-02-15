from MetadataModel import MetadataModel
import pandas as pd

class RI:
    
    def __init__(self, DATABASE_NAME):
        self.MetadataModel = MetadataModel(DATABASE_NAME)
        self.ColumnSimilarities = self.check_column_similarities(self)
              
    def find_source_of_table(self):            
        tableNames = self.MetadataModel.DATABASE_CONN.check_only_table_names_in_database()            
        primary_keys_of_tables = []
        source_primary_keys_of_data_model = self.MetadataModel.DATABASE_CONN.find_source_primary_keys_of_data_model()
            
        for tableName in tableNames:
              primaryKeys = self.MetadataModel.DATABASE_CONN.find_primary_key_of_table(tableName[0])
              primary_keys_of_tables.append([tableName[0],primaryKeys])
            
        tables_which_has_more_keys = list(filter(lambda x : len(x[1]) > 1 , primary_keys_of_tables))
            
        sourceTables = []
        for i in tables_which_has_more_keys:
            for j in i[1]:
                for source_keys in source_primary_keys_of_data_model:
                    if(j[0] == source_keys[1]):
                        sourceTables.append([i[0],source_keys[0],source_keys[1]])
                        
        return sourceTables
    
    def check_relation_between_attributes(self, node_name1 , node_name2, table_name1, table_name2):
        record = self.MetadataModel.NEO4J_CONN.check_relation_between_attributes(node_name1 , node_name2, table_name1, table_name2)
        if(record is None): return False
        else: return True
    
    def check_is_it_single_primary_key(self , attribute_name , entity_name):
        primary_keys = self.MetadataModel.DATABASE_CONN.find_source_primary_keys_of_data_model()
        
        for primary_key in primary_keys:
            if(primary_key[0] == entity_name and primary_key[1] == attribute_name): return True
        else: return False
        
        
    def calculate_similarity_between_columns(self,sourceTableName,sourceColumnName,testTableName,testColumnName):
        match_point = 0
        
        # Checking Column Header Name
        if(self.binarySearchWithLevensiteinDistance(sourceColumnName, testColumnName)): match_point += 2
        
        # Checking Data Types
        sourceColumnDataType = self.MetadataModel.NEO4J_CONN.find_node_data_type(sourceColumnName,sourceTableName)
        testColumnDataType = self.MetadataModel.NEO4J_CONN.find_node_data_type(testColumnName,testTableName)
        if (sourceColumnDataType == testColumnDataType): match_point += 1
        
        # Checking Distinct Values Of Columns
        sourceColumnDataValues = self.MetadataModel.DATABASE_CONN.find_distinct_values_of_column(sourceTableName, sourceColumnName)
        testColumnDataValues = self.MetadataModel.DATABASE_CONN.find_distinct_values_of_column(testTableName,testColumnName)
        
        def comparison_of_values(sourceColumnDataValues,testColumnDataValues):
            match = 0
            short = 0
            
            # Eğer Kolonun İçerisi Boş ise Direkt False Döndür
            if(len(sourceColumnDataValues) == 0 or len(testColumnDataValues) == 0): return False
            
            if(len(sourceColumnDataValues) > len(testColumnDataValues)): short= len(testColumnDataValues) 
            
            else: short= len(sourceColumnDataValues)
            
            # sourceColumnDataValues = list(map(lambda x : x[0] , sourceColumnDataValues))
            testColumnDataValues = list(map(lambda x : x[0] , testColumnDataValues))
            
            
            for i in sourceColumnDataValues:
                if(str(i[0]) in str(testColumnDataValues)): match += 1

            if(match == short): return True
            else: return False                    

        if(comparison_of_values(sourceColumnDataValues,testColumnDataValues)): match_point += 3
        
        return [sourceTableName,sourceColumnName,testTableName,testColumnName,match_point]        
    
    @staticmethod
    def check_column_similarities(self):
        
        tables_of_data_model = self.MetadataModel.DATABASE_CONN.check_only_table_names_in_database()
        tables_of_data_model = list(map(lambda x : x[0] , tables_of_data_model))
        
        results = []
        
        for sourceTable in tables_of_data_model:
            for testTable in tables_of_data_model:
                if(sourceTable == testTable):continue
                else:
                    
                    columns_of_source_table = self.MetadataModel.DATABASE_CONN.check_only_column_names_in_table(sourceTable)
                    columns_of_source_table = list(map(lambda x : x[0] , columns_of_source_table))
                    
                
                    columns_of_test_table = self.MetadataModel.DATABASE_CONN.check_only_column_names_in_table(testTable)
                    columns_of_test_table = list(map(lambda x : x[0] , columns_of_test_table))
                    
                    
                    for sourceColumn in columns_of_source_table:                       
                        for  testColumn in columns_of_test_table:
                            if(self.check_is_it_single_primary_key(sourceColumn , sourceTable)):
    
                                if(self.MetadataModel.NEO4J_CONN.find_node_data_type(sourceColumn,sourceTable) == 'ntext' or 
                                   self.MetadataModel.NEO4J_CONN.find_node_data_type(testColumn,testTable) == 'ntext' or 
                                   self.MetadataModel.NEO4J_CONN.find_node_data_type(sourceColumn,sourceTable) == 'image' or
                                   self.MetadataModel.NEO4J_CONN.find_node_data_type(testColumn,testTable) == 'image'):
                                    continue
                                
                                result = self.calculate_similarity_between_columns(sourceTable, 
                                                                                   sourceColumn,
                                                                                   testTable, 
                                                                                   testColumn)
                                
                                result.append(self.check_relation_between_attributes(result[1],result[3],result[0],result[2]))
                                print(result)
                                results.append(result)
                            else:
                                continue
        
        
        columns = ["Parent_Table_Name","Parent_Column_Name","Child_Table_Name","Child_Column_Name","Similarity_Point","Is_It_Declared"]
        resultsDF = pd.DataFrame(data = results , columns = columns)
        
        return resultsDF
    
    def binarySearchWithLevensiteinDistance(self,string1, string2):
    
          shorterString = ""
          longerString = ""  
          
          if(len(string1) < len(string2)): 
              shorterString = string1
              longerString = string2         
          else:
              shorterString = string2
              longerString = string1
          
          lo = 0
          hi = len(shorterString)
        
          # binary search for prefix
          while lo < hi:
            # +1 for even lengths
            mid = ((hi - lo + 1) // 2) + lo
        
            if shorterString[:mid] == longerString[:mid]:
              # prefixes equal
              lo = mid
            else:
              # prefixes not equal
              hi = mid - 1
        
          if((len(longerString) - lo) <= 1):
              return True
          else:
              return False
          
    
    def find_system_improvements_potential_relations(self):
        
        column_similarities = self.ColumnSimilarities
        
        potential_relations = column_similarities[column_similarities.Is_It_Declared == False]
        
        
        def _label_advice(row):
            if(row["Similarity_Point"] <= 1):
                 return "Irrevelant"
                
            elif(row["Similarity_Point"] == 2):
                 return "Similar Column Name (Possible Broken Link Occurred)"
                
            elif(row["Similarity_Point"] == 3):
                return "Similar Column Name and Data Type (Possible Broken Link Occurred)"
                
            elif(row["Similarity_Point"] == 4):
                 return "Similar Values and Data Types (Check Column Names)"
            
            elif(row["Similarity_Point"] == 5):
                return "Similar Values and Column Header (Check Data Type)"
                
            elif(row["Similarity_Point"] == 6):
                 return "Perfect Match Should Initialize New Relation"
                
            else:
                return "-"
        
        # SettingWithCopyWarning Uyarısı aldığı için iptal edilmiştir yinede kullanılabilir.
        # potential_relations['Advices'] = potential_relations.apply (lambda row: _label_advice(row), axis=1)
        
        col = potential_relations.apply(_label_advice, axis=1) 
        potential_relations = potential_relations.assign(**{'Advices': col.values}) 
        potential_relations = potential_relations.sort_values(by=['Similarity_Point'],ascending=False)
        
        return potential_relations
    
    
    def find_system_improvements_existing_relations(self):
        
        column_similarities = self.ColumnSimilarities     
        existing_relations = column_similarities[column_similarities.Is_It_Declared == True]
               
        def _label_advice(row):
                           
            if(row["Similarity_Point"] == 4):
                 return "Similar Values and Data Types Recommend to Have Same Column Header"
            
            elif(row["Similarity_Point"] == 5):
                return "Similar Values and Data Types Recommend to Have Same Data Type"
                
            elif(row["Similarity_Point"] == 6):
                 return "Perfect Relation"
                
            else:
                return "-"
                
        col = existing_relations.apply(_label_advice, axis=1) 
        existing_relations = existing_relations.assign(**{'Advices': col.values}) 
        existing_relations = existing_relations.sort_values(by=['Similarity_Point'],ascending=False)
        
        return existing_relations
    


if __name__ == '__main__':
    ri = RI("TestDB")
    # test = ri.find_system_improvements_existing_relations()
    test2 = ri.find_system_improvements_potential_relations()


    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    