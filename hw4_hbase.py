import logging
import datetime
import happybase

logger = logging.getLogger("hbase")
logging.basicConfig(level=logging.INFO)

def date_formatter(date):
    """Format dates so it could be sort correctly in hbase."""
    d = datetime.datetime.strptime(date, '%d %b %Y %H:%M:%S') # e.g. 31 Dec 2001 02:24:51
    return d.strftime('%Y%m%d%H%M%S') # e.g. 20011231022451

def parser_aux(file):
    """Auxilliary function to identify row numbers of relavant fields."""
    result = {}

    with open(file, 'r') as f:
        lines = f.read().splitlines()
    
    line_num = 0
    for line in lines:
        # only write to result if it's first encountered
        if ("Date:" in line) and ('date' not in result):
            result['date'] = line_num # date

        if ("From:" in line) and ('sender' not in result):
            result['sender'] = line_num # sender email

        if ("To:" in line) and ('sendee' not in result):
            result['sendee'] = line_num # sendee email(s)
        # subject line number is tracked because sometimes we have multiple lines of sendees
        if ("Subject:" in line) and ('sendee_end' not in result):
            result['sendee_end'] = line_num
        
        # email body starts the next line of X-FileName
        if ("X-FileName:" in line) and ('body' not in result):
            result['body'] = line_num+1 # email body

        line_num += 1
    return result

def email_parser(file):
    """Parse emails for relevant fields."""
    line_num = parser_aux(file)
    result = {}

    with open(file, 'r') as f:
        lines = f.read().splitlines()

    date = lines[line_num['date']].split(',')[1].strip()[:-12]
    result['date'] = date_formatter(date) # date

    result['sender'] = lines[line_num['sender']].split(':')[1].strip() # sender email
    
    if line_num['sendee']<line_num['sendee_end']: # sometimes To: field is missing
        sendee = lines[line_num['sendee']:line_num['sendee_end']] # sendee email(s)
        sendee[0] = sendee[0].split(':')[1]
        result['sendee'] = "".join(sendee)
    else:
        result['sendee'] = ""

    result['body'] = "\n".join(lines[line_num['body']:]) # email body

    return result

if __name__ == "__main__":
    # create connection
    connection = happybase.Connection('127.0.0.1', 49167)
    logger.info("Connection established")

    # create table
    connection.create_table(
        'lf-emails',
        {'allen-p': dict(), # use default config
        'meyers-a': dict()
        }
    )
    logger.info("Table lf-emails created")

    # connect to table
    table = connection.table('lf-emails')
    logger.info("Connected to table lf-emails")

    # data ingestion
    path = '/home/public/enron/'

    files_ap = list(range(1,46))+list(range(62,76))+[78,79]+list(range(83,88))
    files_ap = [path+'allen-p/'+str(x)+'.' for x in files_ap] # file paths for allen-p

    for file in files_ap: # ingest data for allen-p
        result = email_parser(file)
        table.put(bytes(result['date'], encoding='utf-8'),
                {b'allen-p:body': bytes(result['body'], encoding='utf-8'),
                b'allen-p:sender': bytes(result['sender'], encoding='utf-8'),
                b'allen-p:sendee': bytes(result['sendee'], encoding='utf-8')})
        logger.info("Allen-p email %s ingested", file)

    files_ma = list(range(1,23))
    files_ma = [path+'meyers-a/'+str(x)+'.' for x in files_ma] # file paths for meyers-a

    for file in files_ma: # ingest data for meyers-a
        result = email_parser(file)
        table.put(bytes(result['date'], encoding='utf-8'),
                {b'meyers-a:body': bytes(result['body'], encoding='utf-8'),
                b'meyers-a:sender': bytes(result['sender'], encoding='utf-8'),
                b'meyers-a:sendee': bytes(result['sendee'], encoding='utf-8')})
        logger.info("Meyers-a email %s ingested", file)

    # queries
    # 1. body of all emails of meyers-a (scan through all dates/rows)
    answer1 = ""
    for key, data in table.scan(row_start=b'20010911101232', row_stop=b'20020206205713'):
        if b'meyers-a:body' in data: # meyers might not have emails at some dates
            answer1 += str(data[b'meyers-a:body'])

    with open("output1.txt", "w") as text_file:
        text_file.write(answer1)
    logger.info("Query 1 completed and results written to file")

    # 2. body of all emails of 2001 Dec
    answer2 = ""
    for key, data in table.scan(row_prefix=b'200112'):
        if b'allen-p:body' in data:
            answer2 += str(data[b'allen-p:body'])
        if b'meyers-a:body' in data:
            answer2 += str(data[b'meyers-a:body'])
    with open("output2.txt", "w") as text_file:
        text_file.write(answer2)
    logger.info("Query 2 completed and results written to file")

    # 3. body of all emails of allen-p in 2001 Nov
    answer3 = ""
    for key, data in table.scan(row_prefix=b'200111'):
        if b'allen-p:body' in data:
            answer3 += str(data[b'allen-p:body'])
    with open("output3.txt", "w") as text_file:
        text_file.write(answer3)
    logger.info("Query 3 completed and results written to file")

   
