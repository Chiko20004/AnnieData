FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install scraper dependencies
COPY anidata-scraper/requirements.txt /tmp/scraper-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scraper-requirements.txt

# Copy scraper package directly into plugins (importable by Airflow)
COPY anidata-scraper/anidata_scraper /opt/airflow/plugins/anidata_scraper

# Copy DAGs and remaining plugins into the image
COPY airflow/dags /opt/airflow/dags
COPY airflow/plugins /opt/airflow/plugins
