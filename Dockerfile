FROM apache/airflow:2.8.1

COPY . .

ADD requirements.txt .

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt