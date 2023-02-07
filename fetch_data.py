import config 
from datetime import timedelta,time,date,datetime
str_d1 = '01-10-2010'  #input later(small) date
str_d2 = '30-10-2010'  #input earlier(large) date
# str_d2=datetime.today().date()
d1 = datetime.strptime(str_d1, "%d-%m-%Y").date()
d2 = datetime.strptime(str_d2, "%d-%m-%Y").date()
delta= abs(d2 - d1)
df=int(delta.days)
print("days",df)
try:
#fetch data from 10 days table
    def fetch_ten(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                # fetch data from sub table
                    query=f"""select * from empdata_ten where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchall()
                    if len(data)==0:
                # if not in sub table then fetch from main table
                        query1=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                        cur.execute(query1)
                        data=cur.fetchall()
                        for dt in data:
                            print(dt)
                        print('Main 10 table executed')
                    else:
                        for dt in data:
                            print(dt)
                        print('empdata_ten sub table executed')
            connect(cnn)
        
# fetch data from one month table    
    def fetch_1(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
                # fetch data from sub one month table
                    query=f"""select * from empdata1 where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchall()
                    if len(data)==0:
                # if not in sub table then fetch from main table
                        query=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                        cur.execute(query)
                        data=cur.fetchall()
                        for dt in data:
                            print(dt)
                        print('main 1 table execute')
                    else:
                        for dt in data:
                            # li.append(dt)
                            print(dt)
                        print('empdata1 sub table executed')
            connect(cnn)

# fetch data from 3 month table     
    def fetch_3(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
            #select data from 3 month sub table
                    query=f"""select * from empdata3 where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchall()
                    if len(data)==0:
            # if not in sub table then fetch from main table
                        query=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                        cur.execute(query)
                        data=cur.fetchall()
                        for dt in data:
                            print(dt)
                        print('main 3 table execute')
                    else:
                        for dt in data:
                            print(dt)
                        print('empdata3 sub table executed')
            connect(cnn)
            
# fetch data from 6 month table 
    def fetch_6(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
            # fetch from 6 month sub table
                    query=f"""select * from empdata6 where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchall()
                    if len(data)==0:
            # if not in sub table fetch from main table
                        query=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                        cur.execute(query)
                        data=cur.fetchall()
                        for dt in data:
                            print(dt)
                        print('main 6 table execute')
                    else:
                        for dt in data:
                            print(dt)
                        print('empdata6 sub table executed')
            connect(cnn)

# fetch data from 12 month data table table     
    def fetch_12(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
            # fetch data from sub table
                    query=f"""select * from empdata12 where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchone()
                    if len(data)==0:
            # if not in sub table fetch from main table
                        query=f"""select * from empdata_copy where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                        cur.execute(query)
                        data=cur.fetchall()
                        for dt in data:
                            print(dt)
                        print('main 12 table execute')
                    else:
                        for dt in data:
                            print(dt)
                        print('empdata12 sub table executed')
            connect(cnn)
# fetch data from main table 
    def fetch_main(a,b):
        with config.connection() as cnn:
            def connect(cnn):
                with cnn.cursor() as cur:
            # fetch data from main table if the data older than one year.
                    query=f"""select * from empdata where to_timestamp(dates,'MM/DD/YYYY') between '{a}' and '{b}'"""
                    cur.execute(query)
                    data=cur.fetchall()
                    for dt in data:
                        print(dt)
                    print('Only main table execute.')
            connect(cnn)
                
except Exception as err:
    print(err)
    
if(0<=df<=10):
    fetch_6(d1,d2)
elif(10<df<=35): 
    fetch_1(d1,d2)
elif(35<df<=95):
    fetch_3(d1,d2)
elif(95<df<185):
    fetch_6(d1,d2)
elif(185<df<=365):
    fetch_12(d1,d2)
else:
    fetch_main(d1,d2)