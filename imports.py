from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# запись лога от уровня INFO и выше в файл py_log.log + записывается время
# logger.basicConfig(level=logging.INFO, filename="py_log.log",
#                    format="%(asctime)s %(levelname)s %(message)s")
