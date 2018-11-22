#script which queries a database with a query from a text file and outputs
#the results in another text file

#import modules
import sys
import getopt
import sqlite3

def print_help():
    """Print help text."""
    help_text = \
    """Query an SQLite database with query from a text file and return the \
query as a text file.
    
    python query_db.py -d[database] -q[query] -h
    
    Please provide options for database and query.
    
    -d, --database
    Full path to the database, including the file name.
    
    -q, --query
    Full path to the text file containing the query, including the file name.
    
    -h, --help
    Print help.
    """
    
    print(help_text)
    return None

#function to get arguments from the command line
def get_args():
    """Function which gets options to the script passed in the command line."""
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   'd:q:h',
                                   ['database=', 'query=', 'help'])
        
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
        
    if len(args) > 0:
        print("""This function does not take arguments. Please make sure you 
              did not forget to include an option name.""")
        sys.exit()
        
    database = None
    table = None
    query_file = None
        
    for opt, arg in opts:
        if opt in ('-d', '--database'):
            database = arg
        elif opt in ('-q', '--query'):
            query_file = arg
        elif opt in ('-h', '--help'):
            print_help()
            sys.exit()
            
    if database == None | query_file == None:
        print('Please provide a database and a query.')
        sys.exit()
            
    return database, table, query_file

def read_query_file(query_file):
    """Read the content of the query file and return a string to be executed by
    the SQLite cursor."""
    
    #string to be executed by the cursor to query the database
    exec_str = ''
    
    #read the query file and add each line to the executable string
    try:
        with open(query_file, 'r') as file:
            while 1:
                line = file.readline()
                if line == '':
                    break
                else:
                    #add a space to separate command from each line
                    #remove semi-colon for safety and remove line-break
                    exec_str += ' ' + line.replace(';', '').strip('\n')
                    
    except FileNotFoundError as err:
        print(err)
        sys.exit()
    
    #remove the starting space
    exec_str = exec_str.strip()
    
    return exec_str

def execute_query(database, exec_str):
    """Execute a query with the SQLite cursor and return the results."""
    
    #connect to the database
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    
    cur.execute(exec_str)
    
    return None

if __name__ == '__main__':
    database, table, query_file = get_args()
    exec_str = read_query_file(query_file)
    _ = execute_query(exec_str)
    