import psycopg2
from collections import OrderedDict
from django.db import connection
import json
import copy
import logging

logger = logging.getLogger(__name__)

#This class is used to fetch data from the database, and the results are converted into OrderedDict instances to maintain order.
# TODO Add logging an use try-except for failed requests remove dead imports
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
            logger.error(f"Error executing SELECT query: {e}")
            return None
        
    #  Fetches all modules and ids.
    def selectAllModules(self):
        query = f'SELECT id, name  FROM public.modules  ORDER BY id ASC'
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing SELECT query: {e}")
            return None



#  The selectAllmodulesDeatil method fetches all model details.
    def selectAllModulesDetail(self):
         
        query = """
        SELECT 
        m.name, array_agg(mg.group_name) AS groups 
        FROM modules m
        JOIN module_group_map mgm ON m.id = mgm.module_id
        JOIN module_groups mg ON mgm.group_id = mg.id
        GROUP BY m.name
        ORDER BY m.name ASC;
        """
        

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            return column_names, rows
        except Exception as e:
            logger.error(f"Error executing selectAllModulesDeatil query: {e}")
            return None, None


#  The selectModuleMetaData method fetches all model MetaData details.
    def selectModuleMetaData(self, model_type):
        query =  """
        WITH ParamData AS (
            SELECT 
                m.id,
                m.name,
                pt.param_table_name,
                mpm.param_field_id
            FROM 
                modules m
            JOIN 
                param_tables pt ON m.param_id = pt.id
            JOIN 
                model_params_map mpm ON m.id = mpm.id AND pt.id = mpm.param_table_id
            WHERE 
                m.name = %s
        ),
        CalibratableParams AS (
            SELECT 
                pd.name,
                p.name AS param_name,
                p.description AS param_description,
                p.min AS param_min,
                p.max AS param_max,
                p.data_type AS param_data_type,
                p.units AS param_units
            FROM 
                ParamData pd
            LEFT JOIN 
                cfe_params p ON pd.param_table_name = 'cfe_params' AND p.id = pd.param_field_id
            WHERE 
                p.calibratable = true

            UNION ALL

            SELECT 
                pd.name,
                p.name AS param_name,
                p.description AS param_description,
                p.min AS param_min,
                p.max AS param_max,
                p.data_type AS param_data_type,
                p.units AS param_units
            FROM 
                ParamData pd
            LEFT JOIN 
                t_route_params p ON pd.param_table_name = 't_route_params' AND p.id = pd.param_field_id
            WHERE 
                p.calibratable = true

            UNION ALL

            SELECT 
                pd.name,
                p.name AS param_name,
                p.description AS param_description,
                p.min AS param_min,
                p.max AS param_max,
                p.data_type AS param_data_type,
                p.units AS param_units
            FROM 
                ParamData pd
            LEFT JOIN 
                noah_owp_modular_params p ON pd.param_table_name = 'noah_owp_modular_params' AND p.id = pd.param_field_id
            WHERE 
                p.calibratable = true
        )
        SELECT 
            m.name,
            cp.param_name,
            cp.param_description,
            cp.param_min,
            cp.param_max,
            cp.param_data_type,
            cp.param_units,
            ov.name AS output_var_name,
            ov.description AS output_var_description
        FROM 
            modules m
        LEFT JOIN 
            CalibratableParams cp ON m.module_name = cp.module_name
        LEFT JOIN 
            output_variables ov ON m.id = ov.id
        WHERE 
            m.module_name = %s
        ORDER BY 
            cp.param_name, ov.name;
        """
        try:
            self.cursor.execute(query, (model_type,))
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Debug output
            logger.debug("Column names:", column_names)
            logger.debug("Rows returned:", len(rows))
            for row in rows:
                logger.debug(row)

            return column_names, rows
        except Exception as e:
            logger.error(f"Error executing selectModuleMetaData query: {e}")
            return None, None

        
    #  The selectInitialParameters method fetches initial parameters for a given model.
    def selectInitialParameters(self, Moduel):
        model_name = Moduel.upper()
        query1 = """
        SELECT id FROM public.modules
        WHERE name  = %s""" 
        
    
        query2 = """
        SELECT param_id FROM public.modules
        WHERE name  = %s"""
        

        query3 = """
        SELECT  param_table_name FROM public.param_tables
        WHERE id = %s"""
        
        try:
            self.cursor.execute(query1, (model_name,))
            ModelID = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectModelID query: {e}")
            return None, None
        try:
            self.cursor.execute(query2, (model_name,))
            ParamID = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectParamID query: {e}")
            return None, None
        

        try:
            self.cursor.execute(query3, (ParamID,))
            ParamTblName = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectParamName query4: {e}")
            return None, None
        
        
        #SELECT sp.name, sp.units, sp.limits, sp.role 
        query4_1 = """
        SELECT * 
        FROM public."""+ParamTblName 
        query4_2 = """ tn
        WHERE tn.id IN (
            SELECT mpm.param_field_id
            FROM public.module_params_map mpm
            JOIN public.modules mdl ON mdl.id = mpm.module_id
            WHERE mdl.id = %s
        )
        """
        query4 = query4_1 + query4_2

        try:
            self.cursor.execute(query4, (ModelID,))
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            return column_names, rows
        except Exception as e:
            logger.error(f"Error executing selectInitialParamete4rs query: {e}")
            return None, None

    def getModelParametersTotalCount(self, model_type):
        model_name = model_type.upper()
        query1 = """
        SELECT param_id FROM public.modules
        WHERE name  = %s"""

        query2 = """
        SELECT  param_table_name FROM public.param_tables
        WHERE id = %s"""

        query3 = """
        SELECT id FROM public.modules
        WHERE name  = %s"""

        try:
            self.cursor.execute(query1, (model_name,))
            ParamID = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectParamID query: {e}")
            return None, None
        

        try:
            self.cursor.execute(query2, (ParamID,))
            ParamTableName = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectParamName query: {e}")
            return None, None
        
        try:
            self.cursor.execute(query3, (model_name,))
            ModelID = self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error executing selectModelID query: {e}")
            return None, None
        
        query4_1 = """SELECT COUNT(*) FROM public."""+ParamTableName
        query4_2 = """ tn
        WHERE tn.id IN (
            SELECT mpm.param_field_id
            FROM public.module_params_map mpm
            JOIN public.modules mdl ON mdl.id = mpm.module_id
            WHERE mdl.id = %s
        )
        """

        query4 = query4_1 + query4_2

        try:
            self.cursor.execute(query4, (ModelID,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error executing selectInitialParameters query: {e}")
            return None

      
    def update(self, table, set_clause, where_clause=None, params=None):
        query = f'UPDATE "{table}" SET {set_clause}'
        if where_clause:
            query += f' WHERE {where_clause}'
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            logger.debug(f"{self.cursor.rowcount} row(s) updated")
        except Exception as e:
            logger.error(f"Error executing UPDATE query: {e}")
            self.connection.rollback()

    #  The delete method executes a DELETE query.change_me
            self.cursor.execute(query, params)
            self.connection.commit()
            logger.debug(f"{self.cursor.rowcount} row(s) deleted")
        except Exception as e:
            logger.error(f"Error executing DELETE query: {e}")
            self.connection.rollback()


    def selectDependentModuleCalibrateData(self, model_type):
        # below query returns -> 1 | cfe_params, 8 | sft_params, for model_type='SFT'
        query = (f"SELECT distinct pt.id, pt.param_table_name FROM param_tables pt "
                 f"join module_params_map mpm on mpm.param_table_id = pt.id  "
                 f"where mpm.module_id in (select id from modules mods where mods.name = '{model_type}')")
        logger.debug(query)
        try:
            self.cursor.execute(query, (model_type,))
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]

            # Debug output
            logger.debug("Column names:", column_names)
            logger.debug("Rows returned:", len(rows))
            subquery_rowset = []
            for row in rows:
                sub_query = (
                    f"SELECT p.name, p.description, p.min, p.max, p.data_type, p.units, p.calibratable, p.default_value from {row[1]} p "
                    f"join module_params_map mpm on mpm.param_field_id = p.id "
                    f"where p.calibratable = true and mpm.param_table_id = {row[0]} "
                    f"and mpm.module_id in (select id from modules mods where mods.name = '{model_type}')")

                logger.debug(f"sub_query = {sub_query}")
                self.cursor.execute(sub_query, (row, model_type,))
                subquery_rows = self.cursor.fetchall()
                logger.debug(subquery_rows)
                for record in subquery_rows:
                    deep_copy_rec = copy.deepcopy(record)
                    subquery_rowset.append(deep_copy_rec)

            subQuery_rowwset_column_names = [desc[0] for desc in self.cursor.description]
            logger.debug(subquery_rowset)

            return subQuery_rowwset_column_names, subquery_rowset
        except Exception as e:
            status_str = f"Error executing selectDependentModuleCalibrateData query: {e} \n {query}"
            logger.error(status_str)
            return None, None


    def selectModuleCalibrateData(self, model_type):
        table_name = ""
        if model_type in ["CFE-S", "CFE-X"]:
            table_name = "cfe_params"
        elif model_type == "T-Route":
            table_name = "t_route_params"
        elif model_type == "Noah-OWP-Modular":
            table_name = "noah_owp_modular_params"
        elif model_type == "Snow-17":
            table_name = "snow17_params"
        elif model_type == "Sac-SMA":
            table_name = "sac_sma_params"
        elif model_type == "TopModel":
            table_name = "topmodel_params"
        elif model_type == "LASAM":
            table_name = "lasam_params"
        elif model_type == "UEB":
            table_name = "ueb_params"
        else:
            return None, None

        query = f"""
        SELECT 
            p.name, 
            p.description, 
            p.min, 
            p.max, 
            p.data_type, 
            p.units, 
            p.calibratable,
            p.default_value 
        FROM 
            modules m
        JOIN 
            module_params_map map ON m.id = map.module_id
        JOIN 
            {table_name} p ON map.param_field_id = p.id
        JOIN 
            param_tables pt ON pt.id = m.param_id
          WHERE 
            m.name =  '{model_type}' 
            AND p.calibratable = true
        """

        try:
            self.cursor.execute(query, (model_type,))
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Debug output
            logger.debug("Column names:", column_names)
            logger.debug("Rows returned:", len(rows))
            for row in rows:
                logger.debug(row)

            return column_names, rows
        except Exception as e:
            logger.error(f"Error executing selectModuleCalibrateData query: {e}")
            return None, None


    def selectModuleOutVariablesData(self, model_type):
        query = """
            SELECT 
                ov.name,
                ov.description
            FROM 
                output_variables ov
            JOIN 
                modules m ON ov.module_id = m.id
            WHERE 
                m.name = %s
            ORDER BY 
                ov.name;
        """
        try:
            self.cursor.execute(query, [model_type])
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            return column_names, rows
        except Exception as e:
            logger.error(f"Error executing selectModuleOutVariablesData query: {e}")
            return None, None



# Usage test example
if __name__ == "__main__":
    db = DatabaseManager('hydrofabric_db', 'raghav.vadhera', 'change_me', '10.6.0.173')

    db.connect()

    # SELECT example
    # result = db.select('Initial-Parameters', 'Model = %s', ('CFE-S',))
    modules = db.selectAllModules()
    logger.debug(modules)
    #column_names,rows = db.selectInitialParameters('CFE-S')
    
    # # Replacing single quotes to doublw quotes for valuesmoduleMetaData
    # column_names = json.dumps(column_names)
    # logger.debug("Column names:", columinitial_parametersn_names)
    # rows = json.dumps(rows)
    # logger.debug("rows:", rows)    initial_parameters
    # result_dict = [OrderedDict(zip(column_names, row)) for row in rows]
    # result_dict = json.dumps(result_dict)
    # logger.debug the dictionary
    # logger.debug(result_dict)

    # # UPDATE example
    # db.update('Initial-Parameters', 'Model = %s', 'Name = %s', ('CFE-S', 'soil_params.mult'))

    # # DELETE example
    # db.delete('Initial-Parameters', 'Name = %s', ('soil_params.depth',))

    db.close()
