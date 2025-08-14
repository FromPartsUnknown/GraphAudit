import yaml

class ConfigOptionsError(Exception):
    pass

class ConfigOptions(dict):
    def __init__(self, file_path='config.yaml'):
        self.from_file(file_path)

    def __getitem__(self, key):
        return self._config_data[key]
    
    @property
    def values(self):
        return self._config_data

    def from_file(self, yaml_file):
        try:
            try:
                with open(yaml_file, 'r') as fp:
                    self._config_data = yaml.safe_load(fp)
            except yaml.YAMLError as e:
                raise ConfigOptionsError(f"Invalid yaml configuration {yaml_file}: {str(e)}") from e
        except Exception as e:
            raise ConfigOptionsError(f"ConfigOptions: error: {str(e)}") from e
        
    def get_path(self, path):
        keys = path.split('.')
        config = self._config_data
        for key in keys:
            if isinstance(config, dict):
                config = config.get(key)
            else:
                print("Could not find")
                return None
        return config
    