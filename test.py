from unittest.mock import AsyncMock
import pytest
from main import process_start_command


# пометка, что это асинхронный тест
@pytest.mark.asyncio
async def test_start_handler():
    message = AsyncMock()
    await process_start_command(message)
    message.answer.assert_called_with('Привет, пользователь')
