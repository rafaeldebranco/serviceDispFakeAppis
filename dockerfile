# Use an official lightweight Python image
FROM python:3.10.21-slim-bookworm

# Set the working directory inside the container
WORKDIR /home

# Copy dependencies first to leverage Docker's caching mechanism
COPY requirements.txt .

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the internal port the app runs on
EXPOSE 1883

# The command to execute when the container starts
#CMD ["python", "./src/app.py"]
#CMD ["gunicorn","--pythonpath","src", "src.app:main"]
CMD ["gunicorn","--pythonpath","src", "app:main"]
