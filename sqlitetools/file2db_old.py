#script which creates a data base and uploads a csv file into it from the
#command line

#import modules
import pandas as pd
import sqlite3
import os
import sys
import getopt
import math #for isnan

def print_help():
    """Print help text."""
    help_text = \
    """Converts a CSV file to a table placed in a SQLite database.
    
    python file2db.py -d[database] -t[table] -f[file] -e[encoding] -h
    
    Please provide at least the options for the table and file.
    
    -d, --database
    Full path to the database, including the file name. If not provided, the
    database will be named 'new_database.sq3' and placed in the same directory
    as the CSV file.
    
    -t, --table
    Name of the table in which to store the CSV file. Must be provided.
    
    -f, --file
    Full path to the CSV file to be put in the database, including the file 
    name. Must be provided.
    
    -e, --encoding
    Encoding used to decode the CSV file. If not provided, will try 'utf-8',
    'latin-1' and 'utf-16'.
    
    -h, --help
    Display help.
    """
    
    print(help_text)
    return None

def get_args():
    """Function which gets arguments passed when the function is run at the
    command line. It returns the database file path, the table name and the
    path for the file to be put in the table. Specify at least the full name 
    (with path) for the file and a table name."""
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'd:t:f:e:h', 
                                   ['database=', 'table=', 'file=', 'encoding=', 
                                    'help'])

    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
        
    if len(args) > 0:
        print("""This function does not take arguments outside options. 
              Please make sure you did not forget to include an option name.""")
        sys.exit()
    
    db_path = None
    tb_name = None
    file_path = None
    encoding = None
    
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-d', '--database'):
            db_path = arg
        elif opt in ('-t', '--t'):
            tb_name = arg
        elif opt in ('-f', '--file'):
            file_path = arg
        elif opt in ('-e', '--encoding'):
            encoding = arg
        else:
            print('Unhandled option')
    
    if file_path == None:
        print('Please provide a path for the file to be put in the dabase.')
        sys.exit()
        
    if tb_name == None:
        print('Please provide a table name.')
        sys.exit()
    
    return db_path, tb_name, file_path, encoding

def file_to_db(db_path=None, tb_name=None, file_path=None, encoding=None):
    """Function which converts a file to a table in a database."""
    
    #check the user provided table name and file
    if tb_name == None:
        print("Please provide a name for the database table.")
        sys.exit()
    if file_path == None:
        print("Please provide a file to be inserted in the database.")
        sys.exit()
    
    #get working directory as the directory of the file to be put in the database
    work_dir = '\\'.join(file_path.split('\\')[:-1])
    
    #if no database name was provided, give the database file a general name
    #saved in the same directory as the file to be put in the database
    if db_path == None:
        db_name = 'new_database.sq3'
        db_path = os.path.join(work_dir, db_name)
    else:
        db_name = db_path.split('\\')[-1]
    
    #create the database if it does not exist already
    if db_name not in next(os.walk(os.getcwd()))[2]:
        #create database file and connect to it, then create cursor
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        print("Created database '{}'".format(db_name))
    else:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
    #determine if the table is already in the database
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for line in cur:
        if tb_name in line:
            print("Table name '{}' already used. Please choose another name."\
                  .format(tb_name))
            sys.exit()

    #import csv file to pandas dataframe
    df = None
    f_name = file_path.split('\\')[-1]
    #if no encoding was provided try utf-8, latin-1 and utf-16
    if encoding == None:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print("Used 'utf-8' to decode the file.")
        except UnicodeDecodeError:
            print("Failed to use 'utf-8' to decode the file.")
            pass
        try:
            df = pd.read_csv(file_path, encoding='latin-1')
            print("Used 'latin-1' to decode the file.")
        except UnicodeDecodeError:
            print("Failed to use 'latin-1' to decode the file.")
            pass
        try:
            df = pd.read_csv(file_path, encoding='utf-16')
            print("Used 'utf-16' to decode the file.")
        except UnicodeDecodeError:
            print("Failed to use 'utf-16' to decode the file.")
            pass
    else:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            print('Could not read the file. Please specify the right encoding.')
            sys.exit()

    #dictionary of sqlite types corresponding to pandas dtypes
    sql_types = {'int64':'INTEGER', 'float64':'REAL', 'object':'TEXT'}

    #list of (column, type) of dataframe
    field_type = [(i, sql_types[str(j)]) for i, j in zip(df.columns, df.dtypes)]

    #replace characters not compatible with SQL syntax by underscore
    for index, value in enumerate(field_type):
        field_type[index] = (value[0].replace(':', '_').replace('.', '_')\
                .replace(' ', '_').replace('-', '_'), value[1])

    #convert spaces of table name to underscore
    tb_name = tb_name.replace(' ', '_')
    
    #define executable string to create table
    fields_str = ''
    for fields in field_type:
        fields_str += '{} {}, '.format(*fields)
    exec_str = 'CREATE TABLE {0} ({1})'.format(tb_name, fields_str.strip(', '))

    #create table
    cur.execute(exec_str)
    print("Inserted table '{}' in the database.".format(tb_name))

    #get field types of database and put them in a list
    field_type = [j for i, j in field_type]
    
    #get column names (fields of the table) and put them in string
    fields = [i.replace(':', '_').replace('.', '_').replace(' ', '_')\
            .replace('-', '_') for i in df.columns]
    field_str = ('{}, ' * len(fields)).strip(', ').format(*fields)
    
    #put dataframe into table, row by row
    #get dataframe shape
    nrows, ncols = df.shape
    for row in range(nrows):
        #get row values (records of the table)
        values = [i for i in df.loc[row, :]]
    
        #add quotation marks to values containing several words
        for index, value in enumerate(values):
            #avoid trying to modify non-text values
            if field_type[index] == 'TEXT':
                #nan is (weirdly) associated with TEXT but it's a float
                #we set them to NULL
                if type(value) != str:
                    if math.isnan(value):
                        values[index] = 'NULL'
                else:
                    #remove apostrophes
                    values[index] = '"' + str(value).replace('"', '') + '"'
    
        #put values in string
        value_str = ('{}, ' * len(values)).format(*values).strip(', ')
    
        #make executable string
        exec_str = 'INSERT INTO {0} ({1}) VALUES({2})'.format(tb_name, field_str,
                                value_str)
    
        #execute query
        cur.execute(exec_str)

    #commit work
    conn.commit()
    print("File '{0}' inserted in the table '{1}' the database '{2}'."\
          .format(f_name, tb_name, db_name))

    #close cursor and connection to database
    cur.close()
    conn.close()

if __name__ == '__main__':
    print('\nRunning file2db...\n')
    db_path, tb_name, file_path, encoding = get_args()
    file_to_db(db_path, tb_name, file_path, encoding)