# -*- encoding: utf-8 -*-
"""
tests.help.test_ogling module

"""
import pytest

import os
import logging
from hio.help import ogling

def test_oglery():
    """
    Test Oglery class instance that builds loggers
    """

    oglery = ogling.Oglery(name="test")
    assert oglery.path is None
    assert oglery.opened == False

    blogger, flogger = oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    oglery.level = logging.DEBUG
    blogger, flogger = oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    oglery = ogling.Oglery(name="test", level=logging.DEBUG, temp=True, reopen=True)
    assert oglery.path.endswith("_test/hio/log/test.log")
    assert oglery.opened == True
    blogger, flogger = oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    oglery.close()
    assert not os.path.exists(oglery.path)
    assert oglery.opened == False

    oglery.reopen(temp=True)
    assert oglery.path.endswith("_test/hio/log/test.log")
    assert oglery.opened == True
    blogger, flogger = oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    oglery.close()
    assert not os.path.exists(oglery.path)
    assert oglery.opened == False

    """End Test"""

def test_init_oglery():
    """
    Test initOglery function for oglery global
    """
    #defined by default in help.__init__ on import of ogling
    assert isinstance(ogling.oglery, ogling.Oglery)
    assert not ogling.oglery.opened
    assert ogling.oglery.level == logging.CRITICAL  # default

    # nothing should log
    blogger, flogger = ogling.oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    # force reinit
    ogling.oglery =  None
    oglery = ogling.initOglery(name="test", level=logging.DEBUG, temp=True, reopen=True)
    assert oglery == ogling.oglery
    assert oglery.opened
    assert oglery.path.endswith("_test/hio/log/test.log")

    blogger, flogger = oglery.getLoggers()
    blogger.debug("Test blogger at debug level")
    flogger.debug("Test flogger at debug level")
    blogger.info("Test blogger at info level")
    flogger.info("Test flogger at info level")
    blogger.error("Test blogger at error level")
    flogger.error("Test flogger at error level")

    oglery.close()
    assert not os.path.exists(oglery.path)
    oglery.level = logging.CRITICAL  # restore
    """End Test"""


if __name__ == "__main__":
    test_oglery()


