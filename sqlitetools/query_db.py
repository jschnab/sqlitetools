#script which queries a database with a query from a text file and outputs
#the results in another text file

#import modules
import sys
import getopt
import sqlite3
import re

def print_help():
    """Print help text."""
    help_text = \
    """Query an SQLite database with query from a string or text file and \
return the query as a text file.
    
    python query_db.py -d[database] -q[query] -h
    
    Please provide options for database and query.
    
    -d, --database
    Full path to the database, including the file name.
    
    -q, --query
    Query string or full path to the text file containing the query, including \
the file name.
    
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
    query = None
        
    for opt, arg in opts:
        if opt in ('-d', '--database'):
            database = arg
        elif opt in ('-q', '--query'):
            query = arg
        elif opt in ('-h', '--help'):
            print_help()
            sys.exit()
            
    if database == None or query == None:
        print('Please provide a database and a query.')
        sys.exit()
            
    return database, table, query

def read_query_str(query):
    """Removes unsafe characters from the query string."""
    
    exec_str = query.replace(';', '').strip('\n')
    
    return exec_str

def read_query_file(query):
    """Read the content of the query file and return a string to be executed by
    the SQLite cursor."""
    
    #string to be executed by the cursor to query the database
    exec_str = ''
    
    #read the query file and add each line to the executable string
    try:
        with open(query, 'r') as file:
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
    """Execute a query with the SQLite cursor and saves the results in a text
    file in the form of a table."""
    
    #connect to the database
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    
    #execute query string
    cur.execute(exec_str)
    
    #get title of table from executable string
    exec_str = exec_str.lower()
    sub_str = re.search('select(.+)from', exec_str)
    title = sub_str.group(1).strip().split(', ')
    
    #get rows of table from cursor
    rows = []
    for line in cur:
        rows.append(line)

    # change empty values to 'NULL' 
    # (empty values give None, which is not a valid string)
    # initialize empty list to be filled with values from rows
    rows2 = [0] * len(rows)
    # loop through rows tuples
    for i in range(len(rows)):
        # if None is in the tuple, copy element one by one
        # and replace None by 'NULL'
        if None in rows[i]:
            elements = [0] * len(rows[i])
            for j in range(len(rows[i])):
                if rows[i][j] == None:
                    elements[j] = 'NULL'
                else:
                    elements[j] = rows[i][j]
            rows2[i] = elements
        else:
            rows2[i] = rows[i]
        
    #get maximum width of query columns to format output string accordingly
    widths = [len(i) for i in title]
    for row in rows2:
        for idx, col in enumerate(row):
            widths[idx] = max(widths[idx], len(str(col)))
    
    #make a string formatted with title names
    str_list = []
    for w in widths:
        str_list.append('{:>' + str(w + 3) + '}')
    title_str = ''.join(str_list).format(*title)
    
    #make an empty string to be formatted with rows from query
    row_str = ''.join(str_list)
        
    #save query results in a text file
    # add results to the query file name to give the output file name
    if sys.platform == 'linux':
        output_file = 'results_' + query.split('/')[-1]
    elif sys.platform == 'win32':
        output_file = 'results_' + query.split('\\')[-1]
    with open(output_file, 'w') as file:
        file.write(title_str + '\n')
        file.write('-' * (sum(widths) + 3 * len(widths)) + '\n')
        for row in rows2:
            file.write(row_str.format(*row) + '\n')
    
    #close database connection
    cur.close()
    conn.close()
    
    #for testing
    return title_str, row_str.format(*rows2[0])

if __name__ == '__main__':
    database, table, query = get_args()
    
    #if the query is in a text file
    if re.search('.txt$', query):
        exec_str = read_query_file(query)
    
    #if query is a string
    else:
        exec_str = read_query_str(query)

    _ = execute_query(database, exec_str)
    
