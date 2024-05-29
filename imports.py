import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.dispatcher.filters import Command
from string import punctuation
from dataclasses import dataclass
from pycbrf import ExchangeRates
import pandas as pd
from datetime import datetime
import asyncio
import aioschedule
from aiogram.types import *
from aiogram import executor
import logging
from logging import getLogger
import pyowm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession