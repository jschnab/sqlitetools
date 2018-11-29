#script which provides tests for file2db.py

import unittest
import sys
import pandas as pd


sys.path.insert(0, '../sqlitetools')

#import functions to be tested
from file2db import create_tb_str
from file2db import row_to_exec_str
from query_db import read_query_file
from query_db import execute_query

class Test_file2db(unittest.TestCase):
    """Test functions from file2db module."""
    
    def test_create_tb_str(self):
        """Is executable string to make the database table correclty generated?"""
        
        #import the testing dataframe
        df = pd.read_csv('df_latin1.csv', encoding='latin-1')
    
        #dictionary of sqlite types corresponding to pandas dtypes
        sql_types = {'int64':'INTEGER', 'float64':'REAL', 'object':'TEXT',
                 'bool':'INTEGER'}
    
        #list of (column, type) of dataframe
        field_type = [(i, sql_types[str(j)]) for i, j in zip(df.columns, df.dtypes)]
        
        ref_str = 'CREATE TABLE test_tb (text TEXT, integer INTEGER, float REAL, bool INTEGER)'
        
        test_str = create_tb_str(field_type, df, 'test_tb')
        
        self.assertEqual(ref_str, test_str)
        
    def test_row_to_exec_str(self):
        """Is executable string to add dataframe row to table correctly generated?"""
        
        #import the testing dataframe
        df = pd.read_csv('df_latin1.csv', encoding='latin-1')
        
        #dictionary of sqlite types corresponding to pandas dtypes
        sql_types = {'int64':'INTEGER', 'float64':'REAL', 'object':'TEXT',
                 'bool':'INTEGER'}
        
        #check first row
        row = 0
        
        #list of (column, type) of dataframe
        field_type = [(i, sql_types[str(j)]) for i, j in zip(df.columns, df.dtypes)]
        
        tb_name = 'test_tb'
        
        field_str = 'text, integer, float, bool'
        
        ref_str = 'INSERT INTO test_tb (text, integer, float, bool) VALUES(row1, 1, 5.0, True)'
        
        test_str = row_to_exec_str(df, row, field_type, tb_name, field_str)
        
        self.assertEqual(ref_str, test_str)
        
    def test_read_query_str(self):
        """Is text file containing the query read correctly?"""
        
        query_file = 'test_query.txt'
        
        ref_str = 'SELECT AVG(float) FROM test_tb WHERE float > 1.0'
        
        test_str = read_query_file(query_file)
        
        self.assertEqual(ref_str, test_str)
        
    def test_execute_query_title(self):
        """Are the results from the database query correctly generated for title?"""
        
        #query string
        exec_str = 'SELECT name, height FROM family WHERE age BETWEEN 20 AND 40 ORDER BY name'
        
        #reference strings
        ref_title = '       name   height'
        
        title_str, row_str = execute_query('test_db.sq3', exec_str)
        
        self.assertEqual(title_str, ref_title)
        
        
    def test_execute_query_row(self):
        """Are the results from the database query correctly generated for row?"""
        
        #query string
        exec_str = 'SELECT name, height FROM family WHERE age BETWEEN 20 AND 40 ORDER BY name'
        
        #reference strings
        ref_row_0 = '   Jonathan      1.8'
        
        title_str, row_str = execute_query('test_db.sq3', exec_str)
        
        self.assertEqual(row_str, ref_row_0)
        