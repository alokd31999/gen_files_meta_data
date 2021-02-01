#https://github.com/docker-library/python/blob/6d7c130fa4030e28a0978c1882d6cb5576fb23bb/3.7/buster/Dockerfile
#https://github.com/ykursadkaya/pyspark-Docker
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.7.9

#FROM tells Docker which image you base your image on
FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

MAINTAINER alok <alokd3@gmail.com>

COPY --from=py3 / /

ARG PYSPARK_VERSION=2.4.7
ARG parquet_tools_version=0.2.4

RUN pip freeze > requirements.txt

RUN ["apt-get", "update"]
RUN ["apt-get", "-y", "install", "vim"]

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

#RUN tells Docker which additional commands to execute.
# Install any needed packages specified in requirements.txt

RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN mkdir /app/logs

#RUN ["python", "download_files.py"]
RUN ["/bin/bash", "file_metadata.sh"]

CMD ["/bin/bash"]