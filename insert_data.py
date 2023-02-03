import config
from datetime import datetime
from datetime import timedelta
try:
    # insert one date data
    def insrt1():
        d1 ='10/31/2010'
        d2 = datetime.strptime(d1,'%m/%d/%Y').date()
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:

                    query='truncate table empdata1'
                    cur.execute(query)
                    query1=F"""select * from empdata where to_timestamp(dates,'MM/DD/YYYY') ='{d2}'"""
                    cur.execute(query1)
                    query_data=cur.fetchall()
                    query2="insert into empdata1(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    for ki in query_data:
                        cur.execute(query2,ki)
                # cnn.close()
            connect(cnn)           
    # insert three dates data                    
    def insrt3():
        d1 ='10/31/2010'
        d2 = datetime.strptime(d1,'%m/%d/%Y').date()
        d3=d2+timedelta(days=2) 
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    query='truncate table empdata3'
                    cur.execute(query)
                    query1=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{d2}' and  '{d3}'"""
                    cur.execute(query1)
                    query_data=cur.fetchall()
                    query2="insert into empdata3(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    for ki in query_data:
                        cur.execute(query2,ki)
            connect(cnn)
            # cnn.close()
    #insert six dates data
    def insrt6():
        d1 ='10/31/2010'
        d2 = datetime.strptime(d1,'%m/%d/%Y').date()
        d3=d2+timedelta(days=5) 
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    query='truncate table empdata6'
                    cur.execute(query)
                    query1=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{d2}' and  '{d3}'"""
                    cur.execute(query1)
                    query_data=cur.fetchall()
                    query2="insert into empdata6(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    for ki in query_data:
                        cur.execute(query2,ki)
            connect(cnn)
            # cnn.close()
                
    #insert 12 dates data
    def insrt12():
        d1 ='10/31/2010'
        d2 = datetime.strptime(d1,'%m/%d/%Y').date()
        d3=d2+timedelta(days=11) 
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                    query='truncate table empdata12'
                    cur.execute(query)
                    query1=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{d2}' and  '{d3}'"""
                    cur.execute(query1)
                    query_data=cur.fetchall()
                    query2="insert into empdata12(sno,crime,number,dates,location,beat,neighborhood) values (%s,%s,%s,%s,%s,%s,%s) "
                    for ki in query_data:
                        cur.execute(query2,ki)
            connect(cnn)
            # cnn.close()
            
except Exception as err:
    print(err)

insrt1()
insrt3()
insrt6()
insrt12()
