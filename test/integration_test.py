#!/usr/bin/env python
import requests
import pytest
import random
import os
SALT = str(random.randint(0,10000))
TARGET_HOST = "service_under_test"

def test_cross_commits():
   assert True, "Dummy test, will never fail"
