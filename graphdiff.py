import json
import hashlib
import logging
from log import log_init

class GraphException(Exception):
    def __init__(self, message, *args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.error("%s", message, exc_info=True)
        super().__init__(message, *args, **kwargs)


class GraphDiff():
    def __init__(self):
        self._hash_registry = {}
        self._hash_results  = {}
        self._logger = log_init(__name__, level=logging.ERROR)


    def make_hash(self, name, fields):
        self._hash_registry[name] = self._hash_fields(fields)


    def _hash_fields(self, fields):
        return lambda obj: (
            None if not any(f in obj for f in fields) else
            hashlib.sha1(
                json.dumps(
                    {f: obj.get(f) for f in fields},
                    sort_keys=True
                ).encode()
            ).hexdigest()
        )
    

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

        result['new']   = new_ids.tolist()
        result['del']   = del_ids.tolist()
        result['mod']   = mod_ids.tolist()
        result['cache'] = cache_df.loc[mod_ids].reset_index()

        self._hash_results[name] = result
        return result 