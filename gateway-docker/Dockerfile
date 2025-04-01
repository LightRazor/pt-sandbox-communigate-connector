FROM python:3.12.5-bookworm
WORKDIR /gateway
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 10001
COPY gateway.py gateway.py 
CMD ["python","gateway.py"]