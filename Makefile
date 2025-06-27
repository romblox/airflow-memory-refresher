venv:
	source .venv/bin/activate

apply-dynamic-dag:
	python include/scripts/dag_generator.py
