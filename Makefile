venv:
	source .venv/bin/activate

apply-dynamic-dag:
	python include/scripts/dag_generator.py

run-with-dev-dependencies:
	_PIP_ADDITIONAL_REQUIREMENTS="xmltodict==0.14.2" docker compose up
