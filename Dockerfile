FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install scraper dependencies
COPY anidata-scraper/requirements.txt /tmp/scraper-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scraper-requirements.txt

# Install scraper package
COPY anidata-scraper /tmp/anidata-scraper
RUN pip install --no-cache-dir /tmp/anidata-scraper

# Copy DAGs and plugins into the image
COPY airflow/dags /opt/airflow/dags
COPY airflow/plugins /opt/airflow/plugins
