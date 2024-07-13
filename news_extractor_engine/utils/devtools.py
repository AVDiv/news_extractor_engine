import os
from functools import wraps
from contextlib import contextmanager

@contextmanager
def dev_mode():
    if os.getenv("ENVIRONMENT") == "development":
        yield
    else:
        # print("This block is only run in development mode.")
        yield None

def only_dev_mode(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if os.getenv("ENVIRONMENT") == "development":
            return func(*args, **kwargs)
        else:
            # print(f"{func.__name__} is only run in development mode.")
            return None
    return wrapper
