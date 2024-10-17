import logging
import os
from dotenv import load_dotenv
import re
import requests
import json
import uvicorn
from fastapi import FastAPI
import urllib3 #Для теста, убирает лишнее сообщение о недоверенном сертификате, в проде лучше убрать 
urllib3.disable_warnings() #Для теста, убирает лишнее сообщение о недоверенном сертификате, в проде лучше убрать 
from pydantic import BaseModel
import threading
import sys

load_dotenv()
serverInterfaceVersion = os.getenv('InterfaceVersion')
CGrootDir = os.getenv('CGRootDir')
ConnectorDir = os.getenv('ConnectorDir')
GatewayURL = os.getenv('GatewayURL')
GatewayPort = os.getenv('GatewayPort')
GatewayFileUploadUri = os.getenv('GatewayFileUploadUri')
GatewayfileRUIDUri = os.getenv('GatewayfileRUIDUri')
ConnectorURL = os.getenv('ConnectorURL')
ConnectorPort = int(os.getenv('ConnectorPort'))
LoggingLevel = os.getenv('LoggingLevel')

def INTF (RUID, Args):
    if Args == serverInterfaceVersion:
        print(f'{RUID} INTF {serverInterfaceVersion}')
        logging.debug(f'Answer: {RUID} INTF {serverInterfaceVersion}')

def FILE (RUID, Args):
    try:
        messageFileName = Args.split(f'/')[1]
        emlName = messageFileName.split('.')[0] + '.eml'
        logging.debug(f'Message Path: {Args}')
        with open(f'{CGrootDir}{Args}', 'rb') as msgFile:
            payloadFile = ConnectorDir + '/files/' + emlName
            with open(payloadFile, 'wb') as emlFile:
                lines = msgFile.readlines()
                emlFile.writelines(lines[6:]) 
        GatewayUpload(RUID, payloadFile)
    except Exception as e:
        logging.error(f'Failed in FileFunc: {repr(e)}')  
        sys.exit(1)



def GatewayUpload(RUID, File):
    try:
        files = [('file', open(File, 'rb'))]
        UploadFileURL= f'{GatewayURL}:{GatewayPort}{GatewayFileUploadUri}'  
        UploadFileResponse = requests.post(UploadFileURL,files=files, verify=False)
        logging.debug(f'Upload: {UploadFileResponse.text}')
        if UploadFileResponse.status_code == 200:
            os.remove(File)
            UploadFileResponseJson = UploadFileResponse.json()
            logging.debug(f'UploadResult: {UploadFileResponseJson["uuid"]}')
            data = {'RUID' : RUID,
                    'UUID' : UploadFileResponseJson["uuid"]
                    }
            payload = json.dumps(data)
            SetFileRUIDURL= f'{GatewayURL}:{GatewayPort}{GatewayfileRUIDUri}'
            SetFileRUIDURLResponse = requests.post(SetFileRUIDURL, data=payload, verify=False)
            logging.debug(f'SetRUIDResponse: {SetFileRUIDURLResponse.text}')
        else:
            raise ValueError(f'Upload return code: {UploadFileResponse.status_code}')   
    except ValueError as e:
        logging.INFO(f'Failed UploadToGateway: {repr(e)}')
        print(f'{RUID} FAILURE')  #Команда не обрабатывается, возвращает ОK и продолжает обработку
    except ConnectionError as e:
            logging.INFO(f'FailedToConnect: {repr(e)}')
            print(f'{RUID} FAILURE')  #Команда не обрабатывается, возвращает ОK и продолжает обработку
    except Exception as e:
        logging.error(f'Failed UploadToGateway: {repr(e)}')  
        sys.exit(1)

app = FastAPI()

class Verdict(BaseModel):
    RUID: str
    ARG: str

@app.post("/verdict/")
async def getVerdict(verdict:Verdict):
        print(f'{verdict.RUID} {verdict.ARG}')
        logging.debug(f'Verdict Sended:{verdict.RUID}, {verdict.ARG}')
        return

def runAPI():
    logging.info("API Thread Started")
    try:
        uvicorn.run(app='connector:app', host=ConnectorURL, port=ConnectorPort, log_config=None) #log_config=None Позволяет использовать настройки для логирования из main(), в документации uvicorn опция отсутствует 
    except Exception as e:
        logging.error(f'Failed stdin: {repr(e)}')  
        sys.exit(1)

def runStdIn():
    logging.info("stdin Thread Started")
    while True:
        data = input()
        logging.debug({data})
        try:
            data_regex = re.compile(r'\d+\s\w{4}\s\S+')
            data_in = data_regex.match(data).group(0)
            logging.debug(f'Found: {data_in}')
            RUID = data_in.split(" ")[0]
            Command = data_in.split(" ")[1]
            Args = data_in.split(" ")[2]    
            if Command == 'INTF':
               INTF(RUID, Args)
            elif Command == 'FILE':
               FILE(RUID, Args)
            else: 
                print(f'{RUID} FAILURE')  #Команда не обрабатывается, возвращает ОK и продолжает обработку
        except AttributeError as e:
            logging.error(f'Failed stdin: {repr(e)}, InputData: {data}')
            continue
        except Exception as e:
            logging.error(f'Failed stdin: {repr(e)}')  
            sys.exit(1)


def main():
    logging.basicConfig(filename=f'{ConnectorDir}/connector.log', level=logging.getLevelName(LoggingLevel), format=' %(asctime)s - %(threadName)s - %(levelname)s - %(message)s', encoding="utf-8")
    logging.info('MainThread Log Started')
    try: 
        API = threading.Thread(target=runAPI, name='Thread-API')
        API.start()
        IN = threading.Thread(target=runStdIn, name='Thread-stdin')
        IN.start()
    except Exception as e:
            logging.error(f'Failed stdin: {repr(e)}')  
            sys.exit(1)

if __name__ == '__main__':
    main()


    
  
    