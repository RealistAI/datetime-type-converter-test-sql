install:
	pip install -r requirements.txt

translate:
	pip install dwh-migration-tools/client
	python translate_sql.py $(flags)

mapping:
	python generate_td_to_bq_mapping.py
