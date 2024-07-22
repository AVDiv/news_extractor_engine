import asyncio
from dotenv import load_dotenv

from news_extractor_engine.app import App


load_dotenv()

app = App()

if __name__ == "__main__":
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt as e:
        asyncio.run(app.stop())
        print("Shutting down...")
        exit(0)
