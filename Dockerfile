FROM python:3.11-slim
ENV TZ=Africa/Tunis
WORKDIR /app
COPY . /app
EXPOSE 7860
CMD ["python", "app.py"]