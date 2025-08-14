from render import ScreenRender
from pathlib import Path
from log import log_init
import yaml

class DetectionFactory():
    def __init__(
        self, 
        graph_data, 
        template_path='detections',
        output_path=None):
        
        self._logger     = log_init(__name__)
        self._detections = []
        self._templates  = self._load_templates(template_path)
        for template in self._templates:
            detection = Detection(template, graph_data, output_path)
            self._detections.append(detection)

    def __iter__(self):
        return iter(self._detections)

    def _load_templates(self, template_path):
        path = Path(template_path)
        templates = []
    
        if path.is_file():
            if path.suffix.lower() in (".yaml", ".yml"):
                with open(path, "r") as fp:
                    self._logger.info(f"Loading detection: {path}")
                    templates.append(yaml.safe_load(fp))
            else:
                self._logger.warning(f"File {path} is not a YAML file, skipping")
        elif path.is_dir():
            files = path.iterdir()
            for file in files:
                if file.suffix.lower() in (".yaml", ".yml"):
                    with open(file, "r") as fp:
                        self._logger.info(f"Loading detection: {file}")
                        templates.append(yaml.safe_load(fp))
    
        else:
            self._logger.error(f"Path {path} does not exist or is not a file/directory")
        return templates


class Detection(ScreenRender):
    def __init__(self, template, graph_data, output_path):
        super().__init__() 
        self._logger = log_init(__name__)
        
        self._results_list = []
        
        self._graph_data  = graph_data
        self._output_path = output_path

        self._name  = template['name']
        self._query = template['query']
        self._output_template = template['output']
        self._description = template['description']
      
    def run(self):
        results = self._graph_data.query(
            self._query, 
            output_format='list'
        )
        if results:
            id_list = tuple({entry[0] for entry in results})
            # Look up service principal objects
            results = self._graph_data.get_sp_by_id(id_list)
            if results:
                self._results_list = results
        
    def print(self):
        if self._results_list:
            self._render_header(self._name, self._description)
            for sp in self._results_list:
                if self._output_path:
                    self._write_manifest(sp, self._output_path)
                self._render_results(sp, self._output_template)
       
        