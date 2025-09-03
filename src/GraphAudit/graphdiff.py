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
        try:
            results = self._hash_results[name]
        
            fp.write(f"Type: {name}\n")
            result = results['mod']
            self._format_creds(fp, "Modified SPs with Client Credentials", result)
       
            result = results['new']
            self._format_creds(fp, "New SPs with Client Credentials     ", result)

        except Exception as e:
            raise GraphException(f"[-] Error logging result: {e}")
           

    def _format_creds(self, fp, title, result):
        fp.write(f"==========[ {title} ]==========\n")
        for _, row in result.iterrows():
            fp.write(f"\tID: {row['id']}, Name: {row['displayName']}\n")
            
            key_cred = row.get('keyCredentials')
            if key_cred and key_cred not in ['[]', '{}', 'null', 'None'] and len(str(key_cred).strip()) > 2:
                fp.write("\t\t[Key Credentials]\n")
                self._format_creds_array(fp, key_cred)
            
            pwd_cred = row.get('passwordCredentials')
            if pwd_cred and pwd_cred not in ['[]', '{}', 'null', 'None'] and len(str(pwd_cred).strip()) > 2:
                fp.write("\t\t[Password Credentials]\n")
                self._format_creds_array(fp, pwd_cred)
        fp.write('\n')


    def _format_creds_array(self, fp, cred_string):
        try:
            creds = json.loads(cred_string) if isinstance(cred_string, str) else cred_string
            
            if not isinstance(creds, list):
                fp.write(f"\t\t\t{creds}\n")
                return
                
            for i, cred in enumerate(creds, 1):
                fp.write(f"\t\t\tCredential #{i}:\n")
                if isinstance(cred, dict):
                    for key, value in cred.items():
                        if isinstance(value, str) and len(value) > 50:
                            formatted_value = f"{value[:60]}..."
                        else:
                            formatted_value = value
                        fp.write(f"\t\t\t\t{key}: {formatted_value}\n")
                else:
                    fp.write(f"\t\t\t\t{cred}\n")
                fp.write("\n") 
        except (json.JSONDecodeError, TypeError) as e:
            fp.write(f"\t\t\t[Error parsing credentials: {e}]\n")
            fp.write(f"\t\t\t{cred_string}\n")

        
    def make_hash(self, name, fields):
        self._hash_registry[name] = self._hash_fields(fields)
        self._fields[name] = fields


    def _hash_fields(self, fields):
        try:
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
        except Exception as e:
            raise GraphException(f"[-] Error logging result: {e}")            


    def _is_obj_value(self, value):
        if value is None:
            return False
        if isinstance(value, str):
            if len(value) == 0 or value in ['[]', '{}', 'null', 'None']:
                return False
        elif isinstance(value, (list, dict)) and len(value) == 0:
            return False
        return True
        
    
    def results(self, name):
        return self._hash_results[name]
    

    def compare(self, name, cache_df, df):
        try:
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
        except Exception as e:
            raise GraphException(f"[-] Error logging result: {e}")            

        return result 