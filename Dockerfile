FROM python:3.10.10-bullseye

WORKDIR /service

COPY requirements.txt requirements.txt
COPY processing_and_loading/ processing_and_loading/
COPY db_logic/ data_processing_and_loading/

COPY app.py app.py

RUN pip3 install -r requirements.txt

# RUN wget http://dropbox.jobtome.com/data/samples/job_graph_matrix.csv

CMD ["python3","-m","/processing_and_loading/run_data_processing.py","host=0.0.0.0"]