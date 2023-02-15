import redis
import json
import ast



class Redis:
    
    def __init__(self):
            
        HOST = "34.122.178.201"
        PORT = 6379
        self.CONN = redis.Redis(host = HOST, port = PORT)
        
    def redis_insertion(self, key:str , value):
        try:
            dct = {"type": value}
            self.CONN.set(key, json.dumps(dct))
            
        except Exception as e:
            print(e)
            
    def redis_query(self, key):
        try:
            value = self.CONN.get(key)
            output = ast.literal_eval(value.decode('utf-8'))
            return output["type"]
        
        except Exception as e:
            print(e)
            
    def flushall(self):
        self.CONN.flushall()
            
