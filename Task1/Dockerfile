# Use an official Python runtime as a parent image
FROM python:3.9
LABEL authors="csiha"

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./main.py /app/main.py
COPY ./task1.py /app/task1.py
COPY ./__init__.py /app/__init__.py
COPY ./tests /app/tests
COPY ./requirements.txt /app/requirements.txt

# Define environment variable
ENV TSV_PATH=/app/data

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt


# Make port 80 available to the world outside this container
EXPOSE 80

VOLUME [ "./data", "/app/data" ]

ENTRYPOINT pytest && pylint
# Run app.py when the container launches
#CMD ["python", "app.py"]