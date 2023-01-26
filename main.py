import psycopg2
import config
import traceback

def  connect():
    try:
        connection=psycopg2.connect(
        user=config.usernames,
        password=config.pswd,
        host=config.hostserver,
        database=config.databasename,
        port=config.portvalue)
        return connection
        cur=connection.cursor()
    except Exception as err:
        print(err)
def crud(connection):
    # with connection.cursor() as cur:
    
    #create table
    cur=connection.cursor()
    script='''create table if not exists students(
        roll_no serial primary key,
        name varchar(255),
        class varchar(255),
        address varchar(255)) '''
    cur.execute(script)
    
    #insert data
    insert_query=('''insert into students (name,class,address) values (%s,%s,%s) ''')
    insert_data=[('sachin','BBA','haryana'),('Mohit','haacker','New delhi')]
    for i in insert_data:
        # cur.execute(insert_query,i)
        print('')
        
    #update data
    updates=('''update students SET address='Palwal' where address=%s''')
    roll=['palwalhekkfjj']
    cur.execute(updates,roll)
    
    #delete query
    delete_data=('delete from students where roll_no=%s')
    data=[3]
    cur.execute(delete_data,data)
    
    # fetch data
    fetch_script=("select * from students")
    cur.execute(fetch_script)
    data=cur.fetchall()
    for i in data:
        print(i)

    cur.close()
    connection.commit()
    connection.close()
if __name__=="__main__":
    crud(connect())
    