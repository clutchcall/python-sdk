import logging
logging.basicConfig(level=logging.DEBUG)
import test_dial
import asyncio

asyncio.run(test_dial.main())
