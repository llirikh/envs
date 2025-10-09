FROM jupyter/base-notebook:latest

RUN python3 -m pip install pyspark==3.5.1

RUN bash -c "chown -R 1000:100 /home/jovyan/work
# RUN apt-get update
# RUN apt-get install -y openjdk-8-jdk

# RUN java -version

CMD start-notebook.py