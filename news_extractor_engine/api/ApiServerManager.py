import asyncio
import threading
import uvicorn

from .routes import api_app

class ApiServerManager(threading.Thread):
  __server: uvicorn.Server
  __host: str
  __port: int

  def __init__(self, *, host:str, port: int,):
    super().__init__()
    self.__host = host
    self.__port = port
    self.__server_config = uvicorn.Config(
      app=api_app,
      host=host,
      port=port,
      log_level="trace",
      log_config="./fastapi.log-config.yaml",
    )
    self.__server = uvicorn.Server(self.__server_config)

  def run(self):
    self.__server.run()

  def join(self, timeout=None):
    self.__server.handle_exit(2, None)
    super().join(timeout=timeout)
