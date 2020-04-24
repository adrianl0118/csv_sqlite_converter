# ==========================================================
#
# Creator: Adrian Lee
# Date: April 20, 2020
#
# Script for importing SQLite3 database data to .csv file or
# vice versa. For either transfer, the source file must exist
# when the program is run--for CSV to SQLite, the SQLite file
# need not already exist when the script is run, but it is
# assumed that column headings in the source CSV and destination
# SQLite file are compatible if it already exists.
#
# ==========================================================

import codecs, os, sqlite3, csv, argparse, numbers, sys
import unicodecsv as csv

def sqlite_to_csv(db_file, csv_file, table_name):

    print('Importing data from SQLite3 database to CSV file...')

    # Try to connect to SQLite db, which is assumed to exist if it is the source, establish cursor
    try:
        con = sqlite3.connect(db_file)
    except:
        e = sys.exc_info()[0]
        print(e)
        sys.exit(1)
    cur = con.cursor()

    # get the table in question
    cur.execute("SELECT * FROM table_name")
    try:
        tbl = cur.fetchall()
    except:
        print('Could not find requested table')
        sys.exit(1)

    # if the table exists, get the column headings in the table
    try:
        cols = cur.execute("PRAGMA table_info('%s')" % tbl).fetchall()
    except:
        print('No columns are present in requested table')
        sys.exit(1)
    
    # if the column headings exist, open CSV in write mode, write headings
    if len(cols) > 0:
        try:
            f = codecs.open(csv_file, 'w', encoding='utf-8')
        except:
            print('Could not open and prep CSV file for writing')
            sys.exit(1)

        writer = csv.writer(f, dialect=csv.excel, quoting=csv.QUOTE_ALL)
        headings = []
        for col in cols:
            col_name = col[1]
            headings.append(col_name)
        writer.writerow(headings)

        # Write all the rows and close CSV file when done
        for row in tbl:
            writer.writerow(row)
        f.closed
        print('Successfully imported data from SQLite3 database to CSV file')


# For importing CSV to SQLite3 db; get column headings and set up DB for import
def get_db(reader:csv.DictReader, cur:sqlite3.Cursor, f, table_name) -> list:
    
    # Get first row of CSV file
    line = next(reader)
    column_desc = {}
    column_headings = []

    for key in line.keys():
        column_headings.append(key)
        if line[key].isdigit():
            column_desc[key] = 'INTEGER'
        elif line[key].lstrip('-').replace('.','',1).isdigit():
            column_desc[key] = 'FLOAT'
        else:
            column_desc[key] = 'TEXT'
    
    # get back to line 0
    f.seek(0)
    next(reader)

    # Create table in SQLite DB from heading name/datatype pairs taken from CSV; if it already exists, will fail
    cur.execute("CREATE TABLE "+table_name+' (' +
                ', '.join(['%s %s' % (key, value) for (key, value) in column_desc.items()]) + ')')

    # return column heading names
    return column_headings


def csv_to_sqlite(db_file, csv_file, table_name):

    print('Importing data from CSV file to SQLite3 database...')

    # attempt to open the target CSV file
    try:
        f = open(csv_file, 'rb')
    except:
        print('Could not open CSV file')
        sys.exit(1)
    
    # set up CSV reader
    reader = csv.DictReader(f, delimiter='|')
    csv.field_size_limit(500*1024*1024)

    # Connect to the database and establish cursor
    try:
        con = sqlite3.connect(db_file)
    except:
        e = sys.exc_info()[0]
        print(e)
        sys.exit(1)
    cur = con.cursor()
    column_headings = get_db(reader, cur, f, table_name)

    # Add data to SQLite3 line by line from CSV
    for line in reader:
        row_vals = []
        print('Importing CSV line number: '+str(reader.line_num))
        for key in line.keys():
            row_vals.append(line[key])
        qmarks = ','.join(['?'] * len(row_vals))
        columns = ','.join(column_headings)
        cur.execute("INSERT INTO " + table_name + " (" + columns + ") VALUES ({qm});".format(qm=qmarks), row_vals)
    
    # Save and close SQLite3 db
    con.commit()
    f.close()
    print('Successfully imported data from CSV file to SQLite3 database')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports a CSV file TO a SQLite3 database or populates a .csv file FROM a SQLite3 database')
    parser.add_argument('--csv', help='path to .csv file')
    parser.add_argument('--direction', help='specify to or from')
    parser.add_argument('--db', help='path to the SQLite3 database file')
    parser.add_argument('--table', help='table name')
    args = parser.parse_args()

    # Use direction keyword to determine which function is called
    if args.direction == 'to':
        csv_to_sqlite(args.db, args.csv, args.table)
    else:
        sqlite_to_csv(args.db, args.csv, args.table)
    
