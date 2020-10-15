FROM bitnami/spark

WORKDIR /app

ADD jobs /app/jobs
ADD run_etl.sh /app/
ADD configs /app/configs
ADD packages.zip /app/packages.zip
ADD postgresql-42.2.17.jar /opt/bitnami/spark/jars/postgresql-42.2.17.jar

CMD ["/bin/bash", "./run_etl.sh"]
