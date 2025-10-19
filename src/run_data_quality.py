#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Application launcher script.
"""
import os
import sys
from airflow.dags.etl_scripts.data_quality import main

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    main()