import pandas as pd 
import pyodbc as odbc
import numpy as np
import jellyfish

class DB:
    
    def __init__(self, DRIVER_NAME, SERVER_NAME, DATABASE_NAME):
        self.DRIVER_NAME = DRIVER_NAME
        self.SERVER_NAME = SERVER_NAME
        self.DATABASE_NAME = DATABASE_NAME
        
        conn_str = f"""
            DRIVER={{{DRIVER_NAME}}};
            SERVER={SERVER_NAME};
            DATABASE={DATABASE_NAME};
            Trust_Connection=yes;
        """
        self.CONNECTION = odbc.connect(conn_str)

    def readDataFromDB(self,Query):
        df = pd.read_sql_query(Query,self.CONNECTION)
        return df
    
    def searchDatabases(self):
        searchDatabaseQuery = """SELECT name FROM master.sys.databases
        WHERE name NOT IN ('master','tempdb','model','msdb')"""
        databaseDF = self.readDataFromDB(searchDatabaseQuery).values
        databaseNames = []
        for i in range(0,len(databaseDF)): databaseNames.append(databaseDF[i][0])           
        return databaseNames
    
    def searchAllTableNames(self):
        databaseNames = self.searchDatabases()
        tmp = []
        for i in databaseNames:
            db = DB('SQL SERVER','DESKTOP-7P8DAD8',i)
            tmp.append(db._searchTableNames())
        return self._concat_list(tmp)
      
    def _searchTableNames(self):
        tableQuery = """SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES x
                    WHERE TABLE_TYPE = 'BASE TABLE'"""
        
        tableList = self.readDataFromDB(tableQuery).values
        tableNames = []
        for i in range(len(tableList)): 
            tableNames.append([self.DATABASE_NAME,tableList[i][0]])
        return tableNames
    
    def searchAllColumnNames(self):
        databaseNames = self.searchDatabases()
        tmp = []
        for i in databaseNames:
            db = DB('SQL SERVER','DESKTOP-7P8DAD8',i)
            tmp.append(db._searchColumnNames())
        return tmp
            
    def _searchColumnNames(self):
        tableNames = self._searchTableNames()
        tmp = []
        for i in tableNames[1]:
            columnQuery = """SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '"""+str(i)+"""'"""
            
            columnList = self.readDataFromDB(columnQuery).values
            for j in range(len(columnList)): 
                tmp.append([self.DATABASE_NAME,i,columnList[j][0]])

           
        return tmp
    
    def _concat_list(self,list):
        generalList = []
        for i in range(0,len(list)):
            for j in range(0,len(list[i])):
                generalList.append(list[i][j])
               
        return generalList
        
    
    def findForeignKeyRelations(self):
        query = """
        SELECT  db_name() as DatabaseName,
        tab1.name AS [table],
        col1.name AS [column],
        tab2.name AS [referenced_table],
        col2.name AS [referenced_column],
        obj.name AS FK_NAME
        FROM sys.foreign_key_columns fkc
        INNER JOIN sys.objects obj
        ON obj.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tab1
        ON tab1.object_id = fkc.parent_object_id
        INNER JOIN sys.schemas sch
        ON tab1.schema_id = sch.schema_id
        INNER JOIN sys.columns col1
        ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
        INNER JOIN sys.tables tab2
        ON tab2.object_id = fkc.referenced_object_id
        INNER JOIN sys.columns col2
        ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
        WHERE db_name() = '"""+str(self.DATABASE_NAME)+"""'"""
        
        foreignKeysList = self.readDataFromDB(query).values
        return foreignKeysList.astype(str)
      
    def selectBaseNoun(self,word):
        selectedWords = ["order","customer","product","account","currency","sale","finance"]
        substrWord = self._findBaseNoun(word)
        tmp = []
        for substr in substrWord:
            for word in selectedWords:
                if(substr == word): tmp.append(substr)
                else: continue
      
        return tmp
        
    def _findBaseNoun(self,text):
        allSubstrings = self._check_all_substrings(text,len(text))
        
        noun = []
        for substr in allSubstrings:
            editedSubstr = self._singularize_and_lower_str(substr)
            if (self._is_it_noun(editedSubstr)): noun.append(editedSubstr)
            else: continue
        
        return noun
        

    def _check_all_substrings(self,s, n):
        tmp = []
        for i in range(n):
            for len in range(i+1,n+1):
                tmp.append(s[i: len]);        
        return tmp
    
    def find_similarities_between_tables(self):
        allTableNames = self.searchAllTableNames()
        similar_tables = []
        for table in allTableNames:
            for table2 in allTableNames:
                if(self.selectBaseNoun(table[1]) == self.selectBaseNoun(table2[1]) and self.selectBaseNoun(table[1])):
                    similar_tables.append([table[0],table[1],table2[0],table2[1]])
                else: continue
        return similar_tables
    
    def check_table_properties(self):
        query = """SELECT x.TABLE_CATALOG,x.TABLE_NAME, y.CREATE_DATE, y.MODIFY_DATE,z.fk,z.pk,z.uni
        FROM INFORMATION_SCHEMA.TABLES x
        INNER JOIN sys.tables y ON x.TABLE_NAME = y.name
        INNER JOIN (SELECT 
        INFORMATION_SCHEMA.TABLES.TABLE_NAME,
        SUM(CASE WHEN INFORMATION_SCHEMA.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END) AS pk,
        SUM(CASE WHEN INFORMATION_SCHEMA.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'UNIQUE' THEN 1 ELSE 0 END) AS uni,
        SUM(CASE WHEN INFORMATION_SCHEMA.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 1 ELSE 0 END) AS fk
        FROM INFORMATION_SCHEMA.TABLES
        LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS ON INFORMATION_SCHEMA.TABLES.TABLE_NAME = INFORMATION_SCHEMA.TABLE_CONSTRAINTS.TABLE_NAME
        GROUP BY
        INFORMATION_SCHEMA.TABLES.TABLE_NAME) AS z ON z.TABLE_NAME = x.TABLE_NAME
        WHERE x.TABLE_NAME != 'sysdiagrams'
        AND x.TABLE_CATALOG = '"""+str(self.DATABASE_NAME)+"""'"""
             
        tables = self.readDataFromDB(query)
        tables = tables.astype({"CREATE_DATE": str}, errors='raise')
        tables = tables.astype({"MODIFY_DATE": str}, errors='raise')
        
        return tables.values
    
    def check_table_count_in_database(self):
        tableCountQuery = """SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME <> 'sysdiagrams'
        AND TABLE_CATALOG = '"""+str(self.DATABASE_NAME)+"""'"""
        
        tableCount = self.readDataFromDB(tableCountQuery).values[0][0]
        return str(tableCount)
    
    def check_table_create_date_database(self):
        tableCreateDateQuery = """SELECT create_date FROM sys.databases WHERE name = '"""+str(self.DATABASE_NAME)+"""'"""       
        tableCreateDate = self.readDataFromDB(tableCreateDateQuery).values[0][0]
        createDate = pd.to_datetime(str(tableCreateDate)) 
        formatDate = createDate.strftime("%d-%m-%y")
        
        return str(formatDate)
    
    def check_database_views(self):
        query = """SELECT x.TABLE_CATALOG,x.TABLE_NAME, y.CREATE_DATE, y.MODIFY_DATE
        FROM INFORMATION_SCHEMA.TABLES x
        INNER JOIN sys.views y ON x.TABLE_NAME = y.name
        WHERE x.TABLE_TYPE = 'VIEW'
        AND x.TABLE_CATALOG = '"""+str(self.DATABASE_NAME)+"""'"""
             
        views = self.readDataFromDB(query)
        views = views.astype({"CREATE_DATE": str}, errors='raise')
        views = views.astype({"MODIFY_DATE": str}, errors='raise')
        return views.values
     
    def check_columns_of_table(self,table):
        queryForColumns = """SELECT TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,DATA_TYPE,IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '"""+str(table)+"""'"""
                
        tmp = self.readDataFromDB(queryForColumns).values
        return tmp
    
    def check_only_table_names_in_database(self):
        queryForTables = """SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES x
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME != 'sysdiagrams'
        AND x.TABLE_CATALOG = '"""+str(self.DATABASE_NAME)+"""'"""
        
        tables = self.readDataFromDB(queryForTables).values
        return tables
    
    def check_only_column_names_in_table(self,table):
        queryForColumns = """SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '"""+str(table)+"""'"""
        
        columns = self.readDataFromDB(queryForColumns)
        return columns.values
    
    
    def find_similar_tables(self):
        tablesList = self.check_only_table_names_in_database()
        columns = []
        for table in tablesList:
            tmp = db.check_only_column_names_in_table(str(table[0]))
            tmp = np.insert(tmp,0,table)
            columns.append(tmp)
        
        tmp = []
        for i in range(0,len(columns)):
            sourceColumn = columns[i]
            for j in range(0,len(columns)):
                match = 0 
                if(sourceColumn[0] == columns[j][0]): continue
                else:
                    for k in range(1,len(sourceColumn)):
                        for l in range(1,len(columns[j])):
                            if(sourceColumn[k] == columns[j][l]):
                                match += 1
                                
                            elif(self._find_tables_have_similar_values(sourceColumn[0],
                                                                      sourceColumn[k],
                                                                      columns[j][0],
                                                                      columns[j][l])):
                                
                                match += 1
                            else: continue
                
                lenSourceTable = len(sourceColumn)
                lenTestTable = len(columns[j])
                shortOne = 0
                if(lenSourceTable > lenTestTable): 
                    shortOne = lenTestTable 
                else: 
                    shortOne = lenSourceTable
                
                if(match > shortOne//2):
                    tmp.append([sourceColumn[0] , columns[j][0]])

        return tmp
    
    def find_primary_key_of_table(self,table):
        query = """SELECT 
        column_name as PRIMARYKEYCOLUMN
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
        AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
        AND KU.table_name='"""+ str(table) + """'"""
        
        primary_key = self.readDataFromDB(query).values
        return primary_key
     
 
    def _find_tables_have_similar_values(self,parentTable,parentColumn,testTable,testColumn):
        
        if(parentTable == 'DatabaseLog' or testTable == 'DatabaseLog'): return False
        
        parentColumnValuesQuery = """SELECT DISTINCT """+ str(parentColumn) + """ FROM """+ str(parentTable)
        parentColumnValues = self.readDataFromDB(parentColumnValuesQuery).values
        
        testColumnValuesQuery = """SELECT DISTINCT """+ str(testColumn) + """ FROM """+ str(testTable)
        testColumnValues = self.readDataFromDB(testColumnValuesQuery).values
        
        shortList = 0
        if(len(parentColumnValues) < len(testColumnValues)): shortList = len(parentColumnValues) 
        else: len(testColumnValues)
        
        match = 0 
        for i in parentColumnValues:
            for j in testColumnValues:
                if(str(i[0]) == str(j[0])): 
                    match+=1
        
        # Referencial Integrity
        if(match == shortList): 
            return True
        else:
            return False

    def find_source_primary_keys_of_data_model(self):
        query = """SELECT 
        TC.TABLE_NAME, KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
        AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
        WHERE TC.TABLE_NAME IN (SELECT 
        TC.TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
        AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        GROUP BY TC.TABLE_NAME
        HAVING COUNT(COLUMN_NAME) = 1)
		AND TC.TABLE_NAME <> 'sysdiagrams'
        """
        
        source_primary_keys = self.readDataFromDB(query).values
        return source_primary_keys
    
    def find_distinct_values_of_column(self,parentTable,parentColumn):       
        query = """SELECT DISTINCT """+ str(parentColumn) + """ FROM """+ str(parentTable)
        values = self.readDataFromDB(query).values
        return values
     
    def find_columns_source_table(self,tableName,columnName):
        
        source_primary_keys = self.find_source_primary_keys_of_data_model()
        for i in range(0,len(source_primary_keys)):
            if(jellyfish.levenshtein_distance(str(source_primary_keys[i][0]), str(columnName)) < 2):
                columnName = source_primary_keys[i][0]
        
        query = """SELECT 
        x.TABLE_NAME,x.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        INNER JOIN (SELECT TC.TABLE_NAME,COLUMN_NAME
        			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        			INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        			ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
        			AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
        			WHERE TC.TABLE_NAME IN (SELECT 
        									TC.TABLE_NAME
        									FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        									INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        									ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
        									AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        									GROUP BY TC.TABLE_NAME
        									HAVING COUNT(COLUMN_NAME) = 1)) x ON x.COLUMN_NAME = KU.COLUMN_NAME
        WHERE TC.TABLE_NAME = '"""+ str(tableName) + """'""" + """
        AND KU.COLUMN_NAME = '"""+ str(columnName) + """'"""
        
        tableName = self.readDataFromDB(query).values
        return tableName

    def find_tables_and_columns(self):
        query = """SELECT TABLE_NAME,COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS"""
        
        tablesAndColumns = self.readDataFromDB(query)
        return tablesAndColumns
    
    
    def create_new_foreign_key_relation(self,sourceTableName,sourceColumnName,childTableName,childColumnName):
        
        FK_Name = "FK_" + sourceTableName + "_" + childColumnName
        foreign_key_name = "(" + childColumnName + ")"
        references_name = sourceTableName + "(" + sourceColumnName + ")"
        
        
        query = f"""ALTER TABLE {childTableName} ADD CONSTRAINT {FK_Name} 
        FOREIGN KEY {foreign_key_name} REFERENCES {references_name}
        ON DELETE CASCADE
        ON UPDATE CASCADE"""
        
        cur = self.CONNECTION.cursor()
        cur.execute("BEGIN TRANSACTION")
        cur.execute(query)
        cur.execute("COMMIT")
        
        return query
     
    def get_data_types_in_database(self):
        query = "SELECT DISTINCT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
        data_types = self.readDataFromDB(query).values
        data_types= list(map(lambda x : x[0], data_types))

        return data_types
        
if __name__ == "__main__":
    db = DB('SQL SERVER','DESKTOP-7P8DAD8','TestDB')
    test = db.searchDatabases()
    # test = db.create_new_foreign_key_relation('SECTION','Section_identifier',"GRADE_REPORT","Section_identifier")
    

    


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    