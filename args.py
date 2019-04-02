#!/usr/bin/env python

from socket import gethostname
from pathlib import Path
from argparse import Namespace

mimic_path = Path('./data/mimic3')
eicu_path = Path('./data/eicu')
