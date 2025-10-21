FROM jupyter/base-notebook:python-3.8

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk

USER $NB_UID

RUN python3 -m pip install pyspark==3.5.7

#CMD ["start-notebook.py"]