# Use the Python 3.8.3-buster base image
FROM python:3.8.3-buster

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the current directory (where the Dockerfile resides) into the container at /app
COPY . .

# Install the Python dependencies listed in requirement.txt
RUN pip install -r requirement.txt

# Specify the command to run the application when the container starts
CMD [ "python", "./main.py" ]
