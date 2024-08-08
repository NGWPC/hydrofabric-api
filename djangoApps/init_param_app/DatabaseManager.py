import psycopg2
from collections import OrderedDict
from django.db import connection
import json

#This class is used to fetch data from the database, and the results are converted into OrderedDict instances to maintain order.
class DatabaseManager:

    def __init__(self,cursor):
        self.cursor = cursor

    # The select method executes a generic SELECT query.
    def select(self, table, where_clause=None, params=None):
        query = f'SELECT * FROM "{table}"'
        if where_clause:
            query += f' WHERE {where_clause}'
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            return None
        
    #  The getModels mand model_idsethod fetches all models and model_ids.
    def selectAllModels(self):
        query = f'SELECT model_id, name FROM public.models  ORDER BY model_id ASC'
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            return None



#  The selectAllModelsDeatil method fetches all model details.
    def selectAllModelsDeatil(self):
         
        query = """
        SELECT 
        m.description,array_agg(mg.group_name) AS groups, m.module_name, m.version_url, m.commit_hash, m.version_number
        FROM models m
        JOIN model_group_map mgm ON m.model_id = mgm.model_id_fk
        JOIN model_groups mg ON mgm.group_id_fk = mg.id
        GROUP BY m.model_id, m.description, m.module_name, m.version_url, m.commit_hash, m.version_number
        ORDER BY m.model_id ASC;
        """
        

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            return column_names, rows
        except Exception as e:
            print(f"Error executing selectAllModelsDeatil query: {e}")
            return None, None








        
    #  The selectInitialParameters method fetches initial parameters for a given model.
    def selectInitialParameters(self, Model):
        model_name = Model.upper()
        query1 = """
        SELECT model_id FROM public.models
        WHERE name = %s""" 
        
    
        query2 = """
        SELECT param_id_fk FROM public.models
        WHERE name = %s"""
        

        query3 = """
        SELECT  param_table_name FROM public.param_tables
        WHERE id = %s"""
        

        try:
            self.cursor.execute(query1, (model_name,))
            ModelID = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectModelID query: {e}")
            return None, None
        try:
            self.cursor.execute(query2, (model_name,))
            ParamID = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectParamID query: {e}")
            return None, None
        

        try:
            self.cursor.execute(query3, (ParamID,))
            ParamTblName = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectParamName query4: {e}")
            return None, None
        
        
        #SELECT sp.name, sp.units, sp.limits, sp.role 
        query4_1 = """
        SELECT * 
        FROM public."""+ParamTblName 
        query4_2 = """ tn
        WHERE tn.id IN (
            SELECT mpm.param_field_id_fk
            FROM public.model_params_map mpm
            JOIN public.models mdl ON mdl.model_id = mpm.model_id_fk
            WHERE mdl.model_id = %s
        )
        """
        query4 = query4_1 + query4_2

        try:
            self.cursor.execute(query4, (ModelID,))
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            return column_names, rows
        except Exception as e:
            print(f"Error executing selectInitialParamete4rs query: {e}")
            return None, None

    def getModelParametersTotalCount(self, model_type):
        model_name = model_type.upper()
        query1 = """
        SELECT param_id_fk FROM public.models
        WHERE name = %s"""

        query2 = """
        SELECT  param_table_name FROM public.param_tables
        WHERE id = %s"""

        query3 = """
        SELECT model_id FROM public.models
        WHERE name = %s"""

        try:
            self.cursor.execute(query1, (model_name,))
            ParamID = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectParamID query: {e}")
            return None, None
        

        try:
            self.cursor.execute(query2, (ParamID,))
            ParamTableName = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectParamName query: {e}")
            return None, None
        
        try:
            self.cursor.execute(query3, (model_name,))
            ModelID = self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error executing selectModelID query: {e}")
            return None, None
        
        query4_1 = """SELECT COUNT(*) FROM public."""+ParamTableName
        query4_2 = """ tn
        WHERE tn.id IN (
            SELECT mpm.param_field_id_fk
            FROM public.model_params_map mpm
            JOIN public.models mdl ON mdl.model_id = mpm.model_id_fk
            WHERE mdl.model_id = %s
        )
        """

        query4 = query4_1 + query4_2

        try:
            self.cursor.execute(query4, (ModelID,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Error executing selectInitialParameters query: {e}")
            return None

        


    # def getModelParametersTotalCount(self, model_type):
    #     model_name = model_type.upper()
    #     query1 = """
    #     SELECT param_id_fk FROM public.models
    #     WHERE name = %s"""
        

    #     query2 = """
    #     SELECT  param_table_name FROM public.param_tables
    #     WHERE id = %s"""

    #     try:
    #         self.cursor.execute(query1, (model_name,))
    #         ParamID = self.cursor.fetchone()[0]1e}")
    #         return None, None
        

    #     try:
    #         self.cursor.execute(query2, (ParamID,))
    #         ParamTableName = self.cursor.fetchone()[0]
    #     except Exception as e:
    #         print(f"Error executing selectParamName query: {e}")
    #         return None, None
        1
    #     SELECT COUNT(id)  FROM public.%s"""       

    #     self.cursor.execute(query3,ParamTableName)
        
    #     result = self.cursor.fetchone()
    #     return result[0] if result else None1

    #  The update method executes an UPDAselectInitialParametersTE query.

    def update(self, table, set_clause, where_clause=None, params=None):
        query = f'UPDATE "{table}" SET {set_clause}'
        if where_clause:
            query += f' WHERE {where_clause}'
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            print(f"{self.cursor.rowcount} row(s) updated")
        except Exception as e:
            print(f"Error executing UPDATE query: {e}")
            self.connection.rollback()

    #  The delete method executes a DELETE query.change_me
            self.cursor.execute(query, params)
            self.connection.commit()
            print(f"{self.cursor.rowcount} row(s) deleted")
        except Exception as e:
            print(f"Error executing DELETE query: {e}")
            self.connection.rollback()


# Usage test example
if __name__ == "__main__":
    db = DatabaseManager('hydrofabric_db', 'raghav.vadhera', 'change_me', '10.6.0.173')

    db.connect()

    # SELECT example
    # result = db.select('Initial-Parameters', 'Model = %s', ('CFE-S',))
    models = db.selectAllModels()
    print(models)
    #column_names,rows = db.selectInitialParameters('CFE-S')
    
    # # Replacing single quotes to doublw quotes for values
    # column_names = json.dumps(column_names)
    # print("Column names:", columinitial_parametersn_names)
    # rows = json.dumps(rows)
    # print("rows:", rows)    initial_parameters
    # result_dict = [OrderedDict(zip(column_names, row)) for row in rows]
    # result_dict = json.dumps(result_dict)
    # Print the dictionary
    # print(result_dict)

    # # UPDATE example
    # db.update('Initial-Parameters', 'Model = %s', 'Name = %s', ('CFE-S', 'soil_params.mult'))

    # # DELETE example
    # db.delete('Initial-Parameters', 'Name = %s', ('soil_params.depth',))

    db.close()