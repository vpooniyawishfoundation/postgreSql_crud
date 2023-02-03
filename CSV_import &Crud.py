import config
import pypyodbc as odbc
import pandas as pd
df = pd.read_csv('atlcrimedata.csv', low_memory=False)
records = df.values.tolist()
try:
    # 1.create table
    def create():
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    scripts = '''create table if not exists empdata12(
                            sno serial primary key,
                            crime varchar(255),
                            number float default 5,
                            dates varchar not null,
                            location varchar(255),
                            beat char(123),
                            neighborhood char(155)) '''
                    cur.execute(scripts)
                    cnn.commit()
                    cnn.close()
        connect(cnn)

    # 2.fetch data from main and insert into other one
    def insrt():
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    #insert new data
                    # query1="select * from empdata_copy where dates= '10/31/2010'"
                    query1="select * from empdata_copy where dates between '10/29/2010' and '10/31/2010' "
                    # query1="select * from empdata_copy where dates between '10/26/2010' and '10/31/2010' "
                    # query1="select * from empdata_copy where dates between '10/20/2010' and '10/31/2010' "
                    cur.execute(query1)
                    query_data=cur.fetchall()
                    # query2="insert into empdata1(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    query2="insert into empdata3(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    # query2="insert into empdata6(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    # query2="insert into empdata12(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    for ki in query_data:
                        cur.execute(query2,ki)
            connect(cnn)
             
    #3.delete some rows           
    def dlt():
         with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    script='delete from empdata where sno=10'
                    cur.execute(script)
                    cnn.close()
            connect(cnn)
                        
    #4.fetch from the sub table
    def fetch_data():
         with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    query='select sno from empdata1'
                    cur.execute(query)
                    for i in cur.fetchall():
                        print(i)
                    cnn.close()
            connect(cnn)
                        
    print('Please choose a number:\n 1 for create\n 2 for insert\n 3 for delete\n 4 for fetch\n 5 for exit')
    ch=int(input('Enter no: '))
    
    if(ch==1):
        create()
    elif(ch==2):
        insrt()
    elif(ch==3):
        dlt()
    elif(ch==4):
        fetch_data()
    elif(ch==5):
        exit()
    else:
        print('Enter valid no...')
                
except Exception as err:
    print(err)
# finally:
    # cnn.commit()
    # cnn.close()
# if __name__=="__main__":
    # crud(cnn)
       