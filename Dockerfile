FROM python:3.10.10-bullseye

WORKDIR /service

COPY requirements.txt requirements.txt
COPY processing_and_loading/ processing_and_loading/
COPY db_logic/ db_logic/

COPY app.py ./app.py
ENV FLASK_APP=app.py
ENV FLASK_ENV=development
ENV PYTHONUNBUFFERED=1
RUN pip3 install -r requirements.txt
RUN ls -a
#RUN python3 /service/processing_and_loading/run_data_processing.py

# RUN wget http://dropbox.jobtome.com/data/samples/job_graph_matrix.csv

CMD [ "flask", "run", "--host=0.0.0.0"]
