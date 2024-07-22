import os
from functools import wraps
from contextlib import contextmanager


def is_dev_mode():
    if ENVIRONMENT == "development":
        return True
    else:
        # print("This block is only run in development mode.")
        return False


def only_dev_mode(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if ENVIRONMENT == "development":
            return func(*args, **kwargs)
        else:
            # print(f"{func.__name__} is only run in development mode.")
            return None

    return wrapper
