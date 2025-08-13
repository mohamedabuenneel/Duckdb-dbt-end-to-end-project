include .env
export $(shell sed 's/=.*//' .env)

# de men 8erha law ana 3andy file fe nafs el folder b nafs el esm "pypi-ingest" 
#mesh hay3ml el command el esmo pypi-ingest

# --database_name $$DATABASE_NAME \

DBT_FOLDER=transform/pypi_metrics/
DBT_TARGET=dev


.PHONY : pypi-ingest format

pypi-ingest: 
	poetry run python3 -m ingestion.pipeline \
	        --start_date $$START_DATE \
	        --end_date $$END_DATE \
	        --pypi_project $$PYPI_PROJECT \
	        --s3_path $$S3_PATH \
	        --aws_profile $$AWS_PROFILE \
	        --gcp_project $$GCP_PROJECT \
	        --timestamp_column $$TIMESTAMP_COLUMN \
	        --destination $$DESTINATION


pypi-transform:
	cd $(DBT_FOLDER) && \
	dbt run \
		--target $(DBT_TARGET) \
		--vars '{"start_date": "$(START_DATE)", "end_date": "$(END_DATE)"}'



# de ma3naha eni ba3ml reformat le python files bta3ty kolha bsbb  el '.' 3an try2 tool 
#esmha ruff

format:ruff format .


test:pytest tests
