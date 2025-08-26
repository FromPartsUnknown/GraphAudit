import json
import duckdb
import sqlite3
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from log import log_init

from kiota_serialization_json.json_serialization_writer_factory import JsonSerializationWriterFactory
from kiota_abstractions.serialization import Parsable
from kiota_abstractions.store import InMemoryBackingStore


class GraphException(Exception):
    def __init__(self, message, *args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.error("%s", message, exc_info=True)
        super().__init__(message, *args, **kwargs)

class GraphData():
    def __init__(self, db_path='graph_data.db', graph_diff=None):
        self.tables  = {}
        self._hash_registry = {}
        self._logger = log_init(__name__, level=logging.ERROR)
        self._graph_diff = graph_diff

        self._db_path = db_path
        self.db = duckdb.connect(':memory:')
        self._load_from_disk(self._db_path)

    @property
    def db_path(self):
        return self._db_path
   

    def fresh(self, refresh_days=7):
        if Path(self.db_path).exists():
            mtime = datetime.fromtimestamp(Path(self.db_path).stat().st_mtime)
            age_days = (datetime.now() - mtime).days
            if age_days < refresh_days:
                return True
        return False



    def _load_from_disk(self, db_path):
        tables = [
            'service_principals', 
            'app_role_assignments', 
            'app_role_assigned_to', 
            'app_roles', 
            'sp_oauth_grants',
            'sp_member_of', 
            'applications'
        ]

        try:
            if not Path(db_path).exists():
                self._logger.info(f"[*] No databse found: {db_path}")
                return
            
            self._logger.info(f"[*] Opening: {db_path}")
            with open(db_path, 'rb') as fp:
                header = fp.read(16)

            if header.startswith(b'SQLite format 3\0'):
                self.db.execute("INSTALL sqlite; LOAD sqlite;")
                self.db.execute("SET sqlite_all_varchar=true")
                for table in tables:
                    # self.db.execute(
                    #     f"CREATE TABLE IF NOT EXISTS {table} AS "
                    #     f"SELECT * FROM sqlite_scan('{db_path}', '{table}')"
                    # )
                    self.db.execute(f"CREATE TABLE IF NOT EXISTS {table}")
                    self.tables[table] = self.db.table(table)
                self._logger.info(f"[+] Loaded sqlite database: {db_path} into memory")
                    
            elif b'DUCK' in header[:16]:
                # Temporarily attach the disk database
                self._logger.info(f"[+] Attached duckdb database: {db_path}")
                self.db.execute(f"ATTACH DATABASE '{db_path}' AS disk_db")
                for table in tables:
                    self.db.execute(f"CREATE TABLE {table} AS SELECT * FROM disk_db.{table}")
                    self.tables[table] = self.db.table(table)
                # Detach the disk database since we've copied the data    
                self.db.execute("DETACH DATABASE disk_db")
                
            else:
                raise GraphException(f"Could not determine file format for {db_path}")

        except Exception as e:
            raise GraphException(f"Error loading database from disk: {str(e)}") from e


    def store_table(
            self, 
            name, 
            df, 
            persist=True,
            sqlite=True
        ):

        try:
           # Perform diff before loading new data
            if self._graph_diff and name in self.tables:
                cache_df = self.tables[name].to_df()
                self._graph_diff.compare(name, cache_df, df)

            if df.empty:
                self._logger.warning(f"[*] Empty dataframe for table {name}.")
                return
            
            # Replace the in-memory table with new DataFrame data
            self.db.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df")
            self.tables[name] = self.db.table(name)
            
            if persist:
                self._persist_to_disk(name)
                if sqlite:
                    conn = sqlite3.connect(f"{self.db_path}.sqlite")
                    df.to_sql(name, conn, if_exists='replace', index=False)
                    conn.close()

            self._logger.info(f"[+] Stored table '{name}' with {len(df)} rows and {len(df.columns)} columns")

        except Exception as e:
            raise GraphException(f"GraphData: Error storing table: {str(e)}") from e  
        
        
    def _persist_to_disk(self, table_name):
        try:
            
            self._logger.info(f"[*] Attaching {table_name}")
            self.db.execute(f"ATTACH DATABASE '{self._db_path}' AS disk_db")

            # Replace the table on disk with the in-memory version
            self.db.execute(f"CREATE OR REPLACE TABLE disk_db.{table_name} AS SELECT * FROM {table_name}")

             # Detach the disk database
            self.db.execute("DETACH DATABASE disk_db")

            self._logger.info(f"[*] Table {table_name} persisted to disk: {self._db_path}")
            
        except Exception as e:
            raise GraphException(f"Error saving table {table_name} to disk: {self.db_path} Error: {str(e)}") from e

    
    def query(self, sql, output_format='dict'):
        try:
            result = self.db.execute(sql)
            if result:
                if output_format == 'df':
                    return result.fetchdf()
                elif output_format == 'list':
                    return result.fetchall()
                elif output_format in ('dict', 'json'):
                    col_names = [desc[0] for desc in result.description]
                    rows = result.fetchall()
                    dict_rows = [dict(zip(col_names, row)) for row in rows]
                    return dict_rows if output_format == 'dict' else json.dumps(dict_rows)
                else:
                    raise GraphException(ValueError(f"Unsupported output_format: {output_format}"))
        except Exception as e:
            # XXX Handle missing tables. Need to fix with proper schema. 
            if "does not exist" in str(e):
                self._logger.info(f"[-] Query returned empty result due to missing table")
                if output_format == 'df':
                    return pd.DataFrame()
                elif output_format == 'list':
                    return []
                elif output_format in ('dict', 'json'):
                    return {} if output_format == 'dict' else json.dumps([])
                else:
                    return []
   
                
    def _convert_to_json_string(self, value):
        if isinstance(value, (list, dict)):
            try:
                return json.dumps(value)
            except TypeError:
                return str(value)
        return value            


    def _jaysonify_embedded_strings(self, obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str):
                    try:
                        obj[key] = json.loads(value)
                    except json.JSONDecodeError:
                        pass
        elif isinstance(obj, list):
            for idx, value in enumerate(obj):
                if isinstance(value, str):
                    try:
                        obj[idx] = json.loads(value)
                    except json.JSONDecodeError:
                        pass
        return obj
                    

    def get_sp_by_id(self, sp_id_list):
        try:
            id_list = ",".join(f"'{id}'" for id in sp_id_list)
            sp_list = self.query(f"SELECT * FROM service_principals WHERE id IN ({id_list})")
            if not sp_list:
                self._logger.warning("[-] No entries found in service_principals")
                return []

            for sp in sp_list:
                sp_id = sp["id"]
                if not sp_id:
                    self._logger.warning("[-] ServicePrincipal is missing id property")
                    continue

                # Fetch import role assignments
                import_ra = self.query(
                    f"""
                        SELECT a.*, COALESCE(r.value, 'No matching role') AS scope
                            FROM (
                                SELECT * FROM app_role_assigned_to WHERE principalId IN ('{sp_id}')
                            ) a
                            LEFT JOIN app_roles r ON lower(a.appRoleId) = lower(r.id) AND r.service_principal_id = a.resourceId
                     """)
                import_ra = self._jaysonify_embedded_strings(import_ra)

                # Fetch export role assignments
                export_ra = self.query(
                    f"""
                        SELECT a.*, COALESCE(r.value, 'No matching role') AS scope
                        FROM (
                            SELECT * FROM app_role_assignments WHERE resourceId IN ('{sp_id}')
                            UNION
                            SELECT * FROM app_role_assigned_to WHERE resourceId IN ('{sp_id}')
                        ) a
                        LEFT JOIN app_roles r ON a.appRoleId = r.id
                    """)
                export_ra = self._jaysonify_embedded_strings(export_ra)

                # OAuth2 grants
                oauth_grants = self.query(
                    f"""
                        SELECT g.*, COALESCE(sp.displayName, 'No matching resource') AS resourceDisplayName
                        FROM sp_oauth_grants g
                        LEFT JOIN service_principals sp ON lower(g.resourceId) = lower(sp.id)
                        WHERE g.service_principal_id IN ('{sp_id}')
                    """)
                oauth_grants = self._jaysonify_embedded_strings(oauth_grants)

                # Application
                app = self.query(f"""
                    SELECT a.*, sp.id AS service_principal_id
                    FROM applications a
                    INNER JOIN service_principals sp ON lower(sp.appId) = lower(a.appId)
                    WHERE sp.id IN ('{sp_id}')
                """)

                app = app[0] if app else app
                if app:
                    app = self._jaysonify_embedded_strings(app)
                    self._app_resource_access_enrich(app)

                # Directory Roles
                directory_roles = self.query(f"""
                    SELECT *
                    FROM sp_member_of
                    WHERE service_principal_id = '{sp_id}'
                """)
                directory_roles = self._jaysonify_embedded_strings(directory_roles)

                sp['appRoleImports'] = import_ra
                sp['appRoleExports'] = export_ra
                sp['oauth2PermissionGrants'] = oauth_grants
                sp['application'] = app
                sp['member_of'] = directory_roles
                sp = self._jaysonify_embedded_strings(sp)

            return sp_list
        
        except Exception as e:
            raise GraphException(f"GrapData: Error running query: {str(e)}") from e
        

    def _app_resource_access_enrich(self, app):
        try:
            if not isinstance(app, dict) or app == {}:
                self._logger.error(f"[-] Invalid Application: {app}")
                return
            relation = self.tables.get('service_principals')
            if not relation:
                raise GraphException(f"Could not find im memory service_principals")
            rra_list = app.get("requiredResourceAccess")
            if not rra_list:
                return
            
            for rra in rra_list:
                resource_app_id = (rra.get("resourceAppId") or "").lower().strip()
                if resource_app_id:
                    rows = relation.filter(f"appId = '{resource_app_id}'").project("displayName").fetchall()
                    rra["resourceDisplayName"] = next((row[0] for row in rows if row), None)

                    relation_approle = self.tables.get('app_roles')
                    for ra in rra.get("resourceAccess"):
                        if ra.get("type") == "Role":
                            role_id = (ra.get("id")).lower().strip()
                            rows = relation_approle.filter(f"id = '{role_id}'").project("value", "description").fetchall()
                            if rows:
                                value, description = rows[0]
                                ra["scope"] = value
                                ra["description"] = description
        except Exception as e:
            raise GraphException(f"Error enriching app['requiredResourceAccess']: {str(e)}") from e
                                

    def kiota_to_json(self, kiota_obj):
        result = {}
        if kiota_obj is None:
            return result

        if isinstance(kiota_obj, Parsable):
            try:
                writer = \
                    JsonSerializationWriterFactory() \
                        .get_serialization_writer('application/json')
                kiota_obj.serialize(writer)

                content = writer.get_serialized_content()
                if isinstance(content, bytes):
                    json_string = content.decode('utf-8')
                else:
                    json_string = content.getvalue().decode('utf-8')

                result['_raw_json'] = json_string
                result = json.loads(json_string)

            except (AttributeError, TypeError, ValueError) as e:
                self._logger.error(f"[-] Error serializing Parsable object {type(kiota_obj).__name__}: {e}")
                result = {"id": str(getattr(kiota_obj, 'id', '')) if hasattr(kiota_obj, 'id') else None}

        elif isinstance(kiota_obj, InMemoryBackingStore):
            try:
                store_data = kiota_obj._store
                json_string = json.dumps(store_data)
                return json_string
            except (AttributeError, TypeError, ValueError) as e:
                self._logger.error(f"[-] Error serializing InMemoryBackingStore: {e}")
                return result

        for key, value in result.items():
            result[key] = self._convert_to_json_string(value)

        return result
    
    
    def _kiota_process_nested(self, obj):
        if isinstance(obj, dict):
            return {k: self.kiota_to_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.kiota_to_json(item) for item in obj]
        return self.kiota_to_json(obj)
    
    
