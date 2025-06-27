import uuid
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DAGS_DIR = ROOT_DIR / "dags"
INCLUDE_DIR = ROOT_DIR / 'include'
TEMPLATE_DIR = INCLUDE_DIR / 'templates'
INPUTS_DIR = INCLUDE_DIR / 'inputs'

env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
template = env.get_template('dag_template.jinja2')


def apply_dynamic_dag():
    for yaml_file in INPUTS_DIR.glob('*.yaml'):
        print(f"Processing: {yaml_file}")

        try:
            with yaml_file.open('r') as file:
                data: dict[str, Any] = yaml.safe_load(file)
                dag_id = data.get('dag_id', str(uuid.uuid4()))
                output_file = DAGS_DIR / f"get_price_{dag_id.lower()}.py"

                with output_file.open('w') as output:
                    output.write(template.render(data))

                print(f"Generated DAG file: {output_file}")
        except Exception as e:
            print(f"Error processing {yaml_file}: {e}")


if __name__ == "__main__":
    apply_dynamic_dag()
