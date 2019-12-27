# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 10:44:08 2019

@author: yazdsous
"""
import os

os.chdir(r"H:\GitHub\import-export-db")
os.getcwd()
import data_core_eforms as dce

dce.application_type(dce.formfields_by_filingId('C03515',conn0)[0])














