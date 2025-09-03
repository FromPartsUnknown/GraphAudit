import json
import hashlib
import logging
from pathlib import Path
from .log import log_init

class GraphException(Exception):
    def __init__(self, message, *args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.error("%s", message, exc_info=True)
        super().__init__(message, *args, **kwargs)


class GraphDiff():
    def __init__(self):
        self._hash_registry = {}
        self._hash_results  = {}
        self._fields        = {}
        self._logger = log_init(__name__, level=logging.ERROR)
      
    

    def log_results(self, log_file='diff_results.txt'):
        try:
            with open(log_file, 'a') as fp:
                for name in self._hash_results:
                    self._write_result(name, fp)
        except Exception as e:
            raise GraphException(f"[-] Error logging result: {e}")

    def _write_result(self, name, fp):
        result = self._hash_results[name]
        fp.write(f"Type: {name}\n")
        fp.write(f"Modified SPs with Client Credentials:\n")
        mod_result = result['mod']
        for _, row in mod_result.iterrows():
            creds = row.get('keyCredentials')
            # Check for meaningful credentials (not empty string, not '[]', not empty list)
            if creds and creds not in ['[]', '{}', 'null', 'None'] and len(str(creds).strip()) > 2:
                fp.write(f"ID: {row['id']}, Name: {row['displayName']}\n\tCredentials: {row['keyCredentials']}\n")
        
        fp.write(f"New SPs with Client Credentials:\n")
        new_result = result['new']
        for _, row in new_result.iterrows():
            creds = row.get('keyCredentials')
            # Check for meaningful credentials (not empty string, not '[]', not empty list)
            if creds and creds not in ['[]', '{}', 'null', 'None'] and len(str(creds).strip()) > 2:
                fp.write(f"ID: {row['id']}, Name: {row['displayName']}\n\tCredentials: {row['keyCredentials']}\n")

        
    def make_hash(self, name, fields):
        self._hash_registry[name] = self._hash_fields(fields)
        self._fields[name] = fields


    def _hash_fields(self, fields):
        # Capture the method reference before creating the lambda
        is_obj_value = self._is_obj_value
        
        return lambda obj: (
            None if not any(f in obj and is_obj_value(obj.get(f)) for f in fields) else
            hashlib.sha1(
                json.dumps(
                    {f: obj.get(f) for f in fields if f in obj and is_obj_value(obj.get(f))},
                    sort_keys=True
                ).encode()
            ).hexdigest()
        )


    def _is_obj_value(self, value):
        if value is None:
            return False
        if isinstance(value, str):
            # Handle string representations of empty collections
            if len(value) == 0 or value in ['[]', '{}', 'null', 'None']:
                return False
        elif isinstance(value, (list, dict)) and len(value) == 0:
            return False
        return True
        
    
    def results(self, name):
        return self._hash_results[name]
    

    def compare(self, name, cache_df, df):
        result = {}

        if cache_df.empty:
            self._logger.error("[-] Empty table. First run?")
            return {}  
        
        hash_func = self._hash_registry.get(name)
        if not hash_func:
            self._logger.error(f"[-] No hash function registered for {name}")
            return {} 

        df = df.copy()
        df["hash"] = df.apply(hash_func, axis=1)

        cache_df = cache_df.copy()
        cache_df["hash"] = cache_df.apply(hash_func, axis=1)
        
        df = df.set_index("id")     
        cache_df = cache_df.set_index("id")

        # Find different types of changes
        cmn_ids = df.index.intersection(cache_df.index)
        new_ids = df.index.difference(cache_df.index)
        new_ids_with_hash = new_ids[df.loc[new_ids]["hash"].notnull()]
        del_ids = cache_df.index.difference(df.index)

        # Compare only rows with common IDs for changes
        if len(cmn_ids) > 0:
            set1_df = df.loc[cmn_ids]
            set2_df = cache_df.loc[cmn_ids]
            diff_mask = (
                            (set1_df["hash"] != set2_df["hash"]) & 
                            (set1_df["hash"].notna()) & 
                            (set2_df["hash"].notna())
                        )
            mod_ids = set1_df.index[diff_mask] 
        else:
            mod_ids = df.index[[]] 

        result['new'] = df.loc[new_ids_with_hash].reset_index()
        result['del'] = cache_df.loc[del_ids].reset_index()
        result['mod'] = cache_df.loc[mod_ids].reset_index()
        self._hash_results[name] = result

        return result 