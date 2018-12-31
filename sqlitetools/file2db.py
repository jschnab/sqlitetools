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

    -n, --new
    Boolean value to indicate if a new table should be created from the table
    name given. Please provide "True" if you want to create the table.
    
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
                                   'd:t:n:f:e:h', 
                                   ['database=', 'table=', 'new=', 'file=', 'encoding=', 
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
    new_table = None
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
        elif opt in ('-n', '--new'):
            new_table = True
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
    
    return db_path, tb_name, new_table, file_path, encoding

def create_tb_str(field_type, df, tb_name):
    """Return a string that creates a SQLite table when executed by the cursors."""

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
    exec_str_tb = 'CREATE TABLE {0} ({1})'.format(tb_name, fields_str.strip(', '))
    
    return exec_str_tb

def row_to_exec_str(df, row, field_type, tb_name, field_str):
    """Convert a row of a CSV file to an executable string for insertion of
    fields in an SQLite database table."""
    
    #get row values (records of the table)
    values = [i for i in df.loc[row, :]]

    #add quotation marks to values containing several words
    # and convert all nan values to NULL
    for index, value in enumerate(values):
        #convert all nan to NULL in INTEGER and FLOAT columns
        if field_type[index] != 'TEXT':
            if math.isnan(value):
                values[index] = 'NULL'

        #avoid trying to modify non-text values
        if field_type[index] == 'TEXT':
            #nan is (weirdly) associated with TEXT but it's a float
            #we set them to NULL
            if type(value) != str:
                if math.isnan(value):
                    values[index] = 'NULL'
            else:
                #remove quotation marks and semi-colon to avoid SQL injection
                values[index] = '"' + \
                str(value).replace('"', '').replace('\'', '').replace(';', '') \
                   + '"'

    #put values in string
    value_str = ('{}, ' * len(values)).format(*values).strip(', ')

    #make executable string
    exec_str = 'INSERT INTO {0} ({1}) VALUES({2})'.format(tb_name, field_str,
                            value_str)
    
    return exec_str

def file_to_db(db_path=None, tb_name=None, new_table=None, file_path=None, encoding=None):
    """Function which converts a file to a table in a database."""
    
    #=========================================#
    #=== perform checks on input variables ===#
    #=========================================#
    
    #check the user provided table name and file
    if tb_name == None:
        print("Please provide a name for the database table.")
        sys.exit()
    if file_path == None:
        print("Please provide a file to be inserted in the database.")
        sys.exit()
    
    #get working directory as the directory of the file to be put in the database
    system = sys.platform
    if system == 'win32':
        work_dir = '\\'.join(file_path.split('\\')[:-1])
    elif system == 'linux':
        work_dir = '/'.join(file_path.split('/')[:-1])
    #if no database name was provided, give the database file a general name
    #saved in the same directory as the file to be put in the database
    if db_path == None:
        db_name = 'new_database.sq3'
        db_path = os.path.join(work_dir, db_name)
    else:
        if system == 'win32':
            db_name = db_path.split('\\')[-1]
        elif system == 'linux':
            db_name = db_path.split('/')[-1]
        
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
    #cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    #for line in cur:
    #    if tb_name in line:
    #        print("Table name '{}' already used. Please choose another name."\
    #              .format(tb_name))
    #        sys.exit()

    #===========================================#
    #=== import csv file to pandas dataframe ===#
    #===========================================#
    
    df = None
    if system == 'win32':
        f_name = file_path.split('\\')[-1]
    elif system == 'linux':
        f_name = file_path.split('/')[-1]
    #if no encoding was provided try utf-8, latin-1 and utf-16
    if encoding == None:
        while True:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print("Used 'utf-8' to decode the file.")
                break
            except UnicodeDecodeError:
                print("Failed to use 'utf-8' to decode the file.")
            try:
                df = pd.read_csv(file_path, encoding='latin-1')
                print("Used 'latin-1' to decode the file.")
                break
            except UnicodeDecodeError:
                print("Failed to use 'latin-1' to decode the file.")
            try:
                df = pd.read_csv(file_path, encoding='utf-16')
                print("Used 'utf-16' to decode the file.")
                break
            except UnicodeDecodeError:
                print("Failed to use 'utf-16' to decode the file.")

            print('Could not decode file, please select encoding')
            sys.exit()
    else:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            print('Could not read the file. Please specify the right encoding.')
            sys.exit()

    #dictionary of sqlite types corresponding to pandas dtypes
    sql_types = {'int64':'INTEGER', 'float64':'REAL', 'object':'TEXT',
                 'bool':'INTEGER'}
    
    #list of (column, type) of dataframe
    field_type = [(i, sql_types[str(j)]) for i, j in zip(df.columns, df.dtypes)]

    #================================#
    #=== create table in database ===#
    #================================#

    if new_table:
        #make executable string to create table in database
        exec_str_tb = create_tb_str(field_type, df, tb_name)

        #create table
        cur.execute(exec_str_tb)
        print("Inserted table '{}' in the database.".format(tb_name))
    
    #============================================#
    #=== put dataframe into table, row by row ===#
    #============================================#

    #get field types of database and put them in a list
    field_type = [j for i, j in field_type]
    
    #get column names (fields of the table) and put them in string
    fields = [i.replace(':', '_').replace('.', '_').replace(' ', '_')\
            .replace('-', '_') for i in df.columns]
    field_str = ('{}, ' * len(fields)).strip(', ').format(*fields)

    #print(field_str)

    #get dataframe shape
    nrows, ncols = df.shape
    for row in range(nrows):
        #make executable string
        exec_str_row = row_to_exec_str(df, row, field_type, tb_name, field_str)
            
        #print(exec_str_row)

        #execute query
        cur.execute(exec_str_row)

    #commit work
    conn.commit()
    print("File '{0}' inserted in the table '{1}' in the database '{2}'."\
          .format(f_name, tb_name, db_name))

    #close cursor and connection to database
    cur.close()
    conn.close()
    
    #for eventual testing
    return None

if __name__ == '__main__':
    print('\nRunning file2db...\n')
    db_path, tb_name, new_table, file_path, encoding = get_args()
    _ = file_to_db(db_path, tb_name, new_table, file_path, encoding)
