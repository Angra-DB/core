FROM erlang:latest

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Initialize EPMD
RUN epmd -daemon

# Make port 80 evailable to the world outside this container
EXPOSE 1234
