from rich.console import Console
from rich.text import Text
from rich.table import Table
from rich.panel import Panel
from rich.align import Align
from rich import print_json
from config import ConfigOptions
from log import log_init
import jmespath
import json


class ScreenRender:
    def __init__(self):
        self.console = Console()
        self.config  = ConfigOptions('render_config.yaml')
        self._logger = log_init(__name__)

    def _render_data_view(self, path, parent_obj):
        prop_map = self.config.get_path(path)
        if not isinstance(prop_map, dict):
            self._logger.error(f"[-] Invalid config map for path: {path}")
            return

        obj_path = path.split('.', 1)[1] if '.' in path else ""
        if obj_path:
            try:
                obj = self._get_obj_by_path(obj_path, parent_obj)
            except ValueError as e:
                return None
        else:
            obj = parent_obj

        if not obj:
            return None

        outer_table = Table(show_header=False, padding=(0, 0), box=None)
        header  = prop_map.get('TITLE', "")
        comment = prop_map.get('COMMENT', "")
        table = Table(
            show_header=True,
            title_justify="left",
            title=f"[bold underline magenta]{header}[/]" if header else None,
            box=None,
            expand=False,
            padding=(0, 1)
        )

        self._add_table_row(path, obj, prop_map, table)

        outer_table.add_row(table)
        if comment:
            outer_table.add_row(f"[dim cyan]{comment}[/]")

        # Blank spacer row
        outer_table.add_row("")
        return outer_table
    
    
    def _add_table_row(
        self,
        path,
        obj_list,
        prop_map,
        table,
        embedded=0,
        depth=0
    ):
        if depth > 10:
            raise ValueError(f"Max recursion depth exceeded at {path}")

        if not isinstance(prop_map, dict):
            return

        if isinstance(obj_list, dict):
            obj_list = [obj_list]
        elif not isinstance(obj_list, list):
            obj_list = [obj_list] if obj_list is not None else []

        for obj in obj_list:
            if not isinstance(obj, dict):
                continue

            embedded_title_added = False

            for prop_name, prop_desc in prop_map.items():
                # Check for nested prop map (dict) — possible recursion
                if isinstance(prop_desc, dict):
                    if prop_desc.get("EXPAND"):
                        embedded_obj = self._get_obj_by_path(prop_name, obj)
                        if self._has_embedded_data(embedded_obj, prop_desc):
                            self._add_table_row(
                                f"{path}.{prop_name}",
                                embedded_obj,
                                prop_desc,
                                table,
                                embedded=1,
                                depth=depth + 1
                            )
                    continue

                if embedded and not embedded_title_added:
                    title = prop_map.get("TITLE")
                    if title:
                        INDENT_UNIT = "  "
                        indent = INDENT_UNIT * (depth)
                        table.add_row(f"{indent}[dim]{title}[/]", "")
                        embedded_title_added = True

                if prop_name in obj:
                    prop_value = obj[prop_name]
                    if prop_value in [None, "", [], {}]:
                        continue

                    if isinstance(prop_value, list):
                        if all(isinstance(v, (str, int, float)) for v in prop_value):
                            prop_value = " ".join(str(v) for v in prop_value)
                        else:
                            prop_value = json.dumps(prop_value, indent=2)

                    if embedded:
                        indent = INDENT_UNIT * (depth+1)
                        table.add_row(f"[dim]{indent}{prop_desc}[/]", str(prop_value))
                    else:
                        table.add_row(f"[dim]{prop_desc}[/]", str(prop_value))

            # Spacer row between objects
            table.add_row(Text("", style="dim"), Text("", style="dim"))


    def _has_embedded_data(self, obj, prop_map):
        if not obj:
            return False
        if isinstance(obj, dict):
            for key, desc in prop_map.items():
                if isinstance(desc, dict):
                    # Nested check
                    if self._has_embedded_data(obj.get(key), desc):
                        return True
                else:
                    val = obj.get(key)
                    if val not in [None, "", [], {}]:
                        return True
        elif isinstance(obj, list):
            for item in obj:
                if self._has_embedded_data(item, prop_map):
                    return True
        return False


    def _get_obj_by_path(self, path, obj):
        try:
            return jmespath.search(path, obj)
        except jmespath.exceptions.JMESPathError as e:
            raise ValueError(f"[-] JMESPath error for path '{path}': {e}")


    def _write_manifest(self, obj, output_path):
        if output_path:
            with open(output_path, 'a') as fp:
                output = json.dumps(obj, indent=3)
                fp.write(output)
                fp.write(output + "\n") 


    def _render_table(self, obj, display, config):
        for entry in config:
            entry_type  = entry.get('type')
            entry_title = entry.get('title')
            if entry_type == 'table' and obj:
                table = Table(title=entry_title, show_header=False, title_style="blue", title_justify="left")
                columns = entry.get("columns")
                max_rows = max(len(column) for column in columns)
                for column in columns:
                    table.add_column(ratio=1)
                for i in range(max_rows):
                    row = []
                    for column in columns:
                        if i < len(column):
                            if column[i] and column[i].get('data_view'):
                                dv = self._render_data_view(column[i].get('data_view'), obj)
                                row.append(dv)
                            else:
                                row.append("")
                    table.add_row(*row)
                display.add_row(table)


    def _render_header(self, name, description):
        self.console.print(f"[bold bright_cyan]{name}[/bold bright_cyan]", justify="center")
        self.console.print(f"[white]{description}[/white]", justify="center")


    def _render_results(self, obj, template):
        display = Table(show_header=False, box=None)
        if display:
            self._render_table(obj, display, template)
            self.console.print(display)