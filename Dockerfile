FROM apache/airflow:2.10.5

# Set timezone to GMT+7
ENV TZ=Asia/Bangkok
USER root
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
USER airflow

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt