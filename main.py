import asyncio
from enum import Enum
from typing import List
from dataclasses import dataclass
from random import choice, randint
from datetime import datetime

@dataclass
class Payload:
    data: str

@dataclass
class Address:
    email: str

@dataclass
class Event:
    recipients: List[Address]
    payload: Payload

class Result(Enum):
    Accepted = 1
    Rejected = 2

async def read_data() -> Event:
    # Имитация чтения данных
    await asyncio.sleep(1)  # Имитация задержки
    return Event(
        recipients=[Address(email=f"user{randint(1, 10)}@example.com") for _ in range(3)],
        payload=Payload(data="Hello, World!")
    )

async def send_data(dest: Address, payload: Payload, max_retries: int = 3) -> Result:
    retries = 0
    while retries < max_retries:
        await asyncio.sleep(randint(1, 3))  # Имитация задержки
        result = choice([Result.Accepted, Result.Rejected])
        if result == Result.Accepted:
            return Result.Accepted
        retries += 1
    return Result.Rejected

async def perform_operation():
    while True:
        event = await read_data()
        tasks = [send_data(recipient, event.payload) for recipient in event.recipients]
        results = await asyncio.gather(*tasks)
        print(f"[{datetime.now()}] Results: {results}")
        await asyncio.sleep(1)  # Добавим задержку перед следующим циклом чтения

# Запуск асинхронного кода
if __name__ == "__main__":
    try:
        asyncio.run(perform_operation())
    except KeyboardInterrupt:
        print("Operation was stopped manually.")

