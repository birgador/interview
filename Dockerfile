FROM python:3.9.16-bullseye

WORKDIR /service

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

# RUN wget http://dropbox.jobtome.com/data/samples/job_graph_matrix.csv

CMD ["python3","-m","app.py","host=0.0.0.0"]

docker run --restart always --publish=7474:7474 --publish=7687:7687 --env NEO4J_AUTH=neo4j/your_password neo4j:5.4.0