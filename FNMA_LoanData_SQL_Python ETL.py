import os
import psycopg2
import timeit

# AWS Pycharm Postgres Connection Query for ETL
hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'xxxxxxx'

conn = psycopg2.connect(
    host=hostname,
    database=database,
    user=username,
    password=pwd
)

cur = conn.cursor()
conn.autocommit = True

# List of CSV files to import
csv_files = [

    'E:/SQL/Data/2003Q3.csv',
    'E:/SQL/Data/2003Q4.csv',
    'E:/SQL/Data/2004Q1.csv',
    'E:/SQL/Data/2004Q2.csv',
    'E:/SQL/Data/2004Q3.csv',
    'E:/SQL/Data/2004Q4.csv',
    'E:/SQL/Data/2005Q1.csv',
    'E:/SQL/Data/2005Q2.csv',
    'E:/SQL/Data/2005Q3.csv',
    'E:/SQL/Data/2005Q4.csv',
    'E:/SQL/Data/2006Q1.csv',
    'E:/SQL/Data/2006Q2.csv',
    'E:/SQL/Data/2006Q3.csv',
    'E:/SQL/Data/2006Q4.csv',
    'E:/SQL/Data/2007Q1.csv',
    'E:/SQL/Data/2007Q2.csv',
    'E:/SQL/Data/2007Q3.csv',
    'E:/SQL/Data/2007Q4.csv',
    'E:/SQL/Data/2008Q1.csv',
    'E:/SQL/Data/2008Q2.csv',
    'E:/SQL/Data/2008Q3.csv',
    'E:/SQL/Data/2008Q4.csv',
    'E:/SQL/Data/2009Q1.csv',
    'E:/SQL/Data/2009Q2.csv',
    'E:/SQL/Data/2009Q3.csv',
    'E:/SQL/Data/2009Q4.csv',
    'E:/SQL/Data/2010Q1.csv',
    'E:/SQL/Data/2010Q2.csv',
    'E:/SQL/Data/2010Q3.csv',
    'E:/SQL/Data/2010Q4.csv',
    'E:/SQL/Data/2011Q1.csv',
    'E:/SQL/Data/2011Q2.csv',
    'E:/SQL/Data/2011Q3.csv',
    'E:/SQL/Data/2011Q4.csv',
    'E:/SQL/Data/2012Q1.csv',
    'E:/SQL/Data/2012Q2.csv',
    'E:/SQL/Data/2012Q3.csv',
    'E:/SQL/Data/2012Q4.csv',
    'E:/SQL/Data/2013Q1.csv',
    'E:/SQL/Data/2013Q2.csv',
    'E:/SQL/Data/2013Q3.csv',
    'E:/SQL/Data/2013Q4.csv',
    'E:/SQL/Data/2014Q1.csv',
    'E:/SQL/Data/2014Q2.csv',
    'E:/SQL/Data/2014Q3.csv',
    'E:/SQL/Data/2014Q4.csv',
    'E:/SQL/Data/2015Q1.csv',
    'E:/SQL/Data/2015Q2.csv',
    'E:/SQL/Data/2015Q3.csv',
    'E:/SQL/Data/2015Q4.csv',
    'E:/SQL/Data/2016Q1.csv',
    'E:/SQL/Data/2016Q2.csv',
    'E:/SQL/Data/2016Q3.csv',
    'E:/SQL/Data/2016Q4.csv',
    'E:/SQL/Data/2017Q1.csv',
    'E:/SQL/Data/2017Q2.csv',
    'E:/SQL/Data/2017Q3.csv',
    'E:/SQL/Data/2017Q4.csv',
    'E:/SQL/Data/2018Q1.csv',
    'E:/SQL/Data/2018Q2.csv',
    'E:/SQL/Data/2018Q3.csv',
    'E:/SQL/Data/2018Q4.csv',
    'E:/SQL/Data/2019Q1.csv',
    'E:/SQL/Data/2019Q2.csv',
    'E:/SQL/Data/2019Q3.csv',
    'E:/SQL/Data/2019Q4.csv',
    'E:/SQL/Data/2020Q1.csv',
    'E:/SQL/Data/2020Q2.csv',
    'E:/SQL/Data/2020Q3.csv',
    'E:/SQL/Data/2020Q4.csv',
    'E:/SQL/Data/2021Q1.csv',
    'E:/SQL/Data/2021Q2.csv',
    'E:/SQL/Data/2021Q3.csv',
    'E:/SQL/Data/2021Q4.csv',
    'E:/SQL/Data/2022Q1.csv',
    'E:/SQL/Data/2022Q2.csv',
    'E:/SQL/Data/2022Q3.csv',
    'E:/SQL/Data/2022Q4.csv',
    'E:/SQL/Data/2023Q1.csv',
    'E:/SQL/Data/2023Q2.csv'

]


def create_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS loan_staging1 CASCADE;
    CREATE UNLOGGED TABLE loan_staging1(
    Pool varchar(25) DEFAULT NULL,
  LoanID varchar(50) DEFAULT NULL,
  Period varchar(25)  DEFAULT NULL,
  Channel varchar(2) DEFAULT NULL,
  Seller varchar(100) DEFAULT NULL,
  Servicer varchar(100) DEFAULT NULL,
  MasterServicer varchar(100) DEFAULT NULL,
  OrigRate decimal(8,4) DEFAULT NULL,
  CurRate decimal(8,4)  DEFAULT NULL,
  OrigUPB decimal(10,2) DEFAULT NULL,
  UPBIssue decimal(10,2) DEFAULT NULL,
  CurUPB decimal(10,2) DEFAULT NULL,
  OrigTerm int DEFAULT NULL,
  OrigDate int DEFAULT NULL,
  FstPayDt int DEFAULT NULL,
  Age int DEFAULT NULL,
  RemLegalTerm int DEFAULT NULL,
  RemTerm int DEFAULT NULL,
  MatDate int DEFAULT NULL,
  OrigLTV decimal(8,4)   DEFAULT NULL,
  OrigCLTV decimal(8,4)  DEFAULT NULL,
  Borrowers int DEFAULT NULL,
  DTI decimal(8,4)  DEFAULT NULL,
  CreditScoreOrig int DEFAULT NULL,
  CoCreditScoreOrig int DEFAULT NULL,
  FstTimeHomeBuy varchar(5) DEFAULT NULL,
  Purpose varchar(2) DEFAULT NULL,
  PropType varchar(2) DEFAULT NULL,
  Units int DEFAULT NULL,
  Occupancy varchar(3) DEFAULT NULL,
  State varchar(2) DEFAULT NULL,
  MSA varchar(5) DEFAULT NULL,
  Zip int DEFAULT NULL,
  MiIns float DEFAULT NULL,
  AmortType varchar(3) DEFAULT NULL,
  PrepayPen varchar(1) DEFAULT NULL,
  IO varchar(1) DEFAULT NULL,
  IOPrinDT int DEFAULT NULL,
  IOAmortTerm int DEFAULT NULL,
  DlqStatus varchar(2) DEFAULT NULL,
  Paystring varchar(48) DEFAULT NULL,
  ModFlag varchar(3) DEFAULT NULL,
  MtgencCaclInsur varchar(2) DEFAULT NULL,
  ZeroBalCode varchar(3) DEFAULT NULL,
  ZeroBalDt varchar(25) DEFAULT NULL,
  UPBLiq decimal(10,2) DEFAULT NULL,
  RepurchDt int DEFAULT NULL,
  SchedPrinCur decimal(10,2) DEFAULT NULL,
  TotPrinCur decimal(10,2) DEFAULT NULL,
  UnschedPrinCur int DEFAULT NULL,
  LastPaidInstalDt int DEFAULT NULL,
  FC_Date int DEFAULT NULL,
  DispDt int DEFAULT NULL,
  FC_Costs decimal(10,2) DEFAULT NULL,
  PropPrevCost decimal(10,2) DEFAULT NULL,
  AssetRecovCost decimal(10,2) DEFAULT NULL,
  MiscHoldingExpCredit decimal(10,2) DEFAULT NULL,
  TaxesHoldProp decimal(10,2) DEFAULT NULL,
  NetSalesProceed decimal(10,2) DEFAULT NULL,
  CreditEnhanceProceeds decimal(10,2) DEFAULT NULL,
  RepurchWholeProceed decimal(10,2) DEFAULT NULL,
  OtherFCProceeds decimal(10,2) DEFAULT NULL,
  ModNonIntUPB decimal(10,2) DEFAULT NULL,
  PrincForgiveAmt decimal(10,2) DEFAULT NULL,
  OrigListStartDt int DEFAULT NULL,
  OrigListPx decimal(10,2) DEFAULT NULL,
  CurListStartDt int DEFAULT NULL,
  CurListPx int DEFAULT NULL,
  CreditScoreIss int DEFAULT NULL,
  CoCreditScoreIss int DEFAULT NULL,
  CreditScoreCur int DEFAULT NULL,
  CoCreditScoreCur int DEFAULT NULL,
  MtgeInsureType varchar(45) DEFAULT NULL,
  ServActivity varchar(45) DEFAULT NULL,
  CurPerModLoss decimal(10,2) DEFAULT NULL,
  CumPerModLoss decimal(10,2) DEFAULT NULL,
  CurCreditEventLoss decimal(10,2) DEFAULT NULL,
  CumCreditEventLoss decimal(10,2) DEFAULT NULL,
  SpeciaEligProgram varchar(45) DEFAULT NULL,
  FCPrinWriteOff decimal(10,2) DEFAULT NULL,
  ReloMtge varchar(1) DEFAULT NULL,
  ZeroBalChgDt varchar(25)DEFAULT NULL,
  LoanHoldback varchar(1) DEFAULT NULL,
  LoanHoldbackDT int DEFAULT NULL,
  DlqAccurInt int DEFAULT NULL,
  PropvalMethod varchar(1) DEFAULT NULL,
  HighBal varchar(1) DEFAULT NULL,
  ARMPeriod varchar(1) DEFAULT NULL,
  ArmProduct varchar(100) DEFAULT NULL,
  FstPerCap int DEFAULT NULL,
  PerCapAdjFreq int DEFAULT NULL,
  PerCap int DEFAULT NULL,
  NextPayChngDt int DEFAULT NULL,
  Index varchar(100) DEFAULT NULL,
  ArmCapStrct varchar(10) DEFAULT NULL,
  FstIntRateCapPerc float DEFAULT NULL,
  PerIntRateCapPerc float DEFAULT NULL,
  LifeCap float DEFAULT NULL,
  MtgeMargin float DEFAULT NULL,
  BalloonInd varchar(1) DEFAULT NULL,
  ARMPlan varchar(4) DEFAULT NULL,
  BorrowerAsstPlan varchar(1) DEFAULT NULL,
  HLTVRefiOption varchar(1) DEFAULT NULL,
  DealName varchar(200) DEFAULT NULL,
  RepurchMakeWhole varchar(1) DEFAULT NULL,
  AltDelResol varchar(1) DEFAULT NULL,
  AltDelResCnt varchar(1) DEFAULT NULL,
  DeferralAmt decimal(10,2) DEFAULT NULL
    );
    """)


with conn.cursor() as cursor:
    create_staging_table(cursor)


def send_csv_to_psql(connection, csv, table_):
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '|'"
    with open(csv, 'r') as file:
        with connection.cursor() as cur:
            cur.execute("TRUNCATE " + table_ + ";")  # to eliminate duplicates
            cur.copy_expert(sql=sql % table_, file=file)
            connection.commit()

# Wrap the function calls with timeit to measure execution time
upload_time = timeit.timeit(lambda: [send_csv_to_psql(conn, csv_file, 'fnma_loanhist') for csv_file in csv_files], number=1)

print(f"Data upload took {upload_time:.2f} seconds.")


def staging_Transform(cursor):
    cursor.execute("""

UPDATE fnma_loanhist
Set pk =
CASE When (length(period) = 5) THEN concat(right(period, 4), 0, left(period,1))

ELSE concat(right(period, 4), left(period,2))

END;

UPDATE fnma_loanhist
Set zerobalchgdt =
CASE When (length(zerobaldt) = 5) THEN concat(right(zerobalchgdt, 4), 0, left(zerobaldt,1))

ELSE concat(right(zerobaldt, 4), left(zerobaldt,2))

END;


UPDATE fnma_loanhist
Set period = right(pk,6)
;

Update fnma_loanhist
Set DeferralAmt = Case
When origrate < 2.25 then 1.5
When origrate between 2.25 and 2.7499 then 2.0
When origrate between 2.75 and 3.2499 then 2.5
When origrate between 3.25 and 3.7499 then 3.0
When origrate between 3.75 and 4.2499 then 3.5
When origrate between 4.25 and 4.7499 then 4.0
When origrate between 4.75 and 5.2499 then 4.5
When origrate between 5.25 and 5.7499 then 5.0
When origrate between 5.75 and 6.2499 then 5.5
When origrate between 6.25 and 6.7499 then 6.0
When origrate between 6.75 and 7.2499 then 6.5
When origrate between 7.25 and 7.7499 then 7.0
When origrate between 7.75 and 8.2499 then 7.5
When origrate between 8.25 and 8.7499 then 8.0
When origrate between 8.75 and 9.2499 then 8.5
When origrate >= 9.25 then 9.0

end;

;
""")


with conn.cursor() as cursor:
    staging_Transform(cursor)



# Perform your transformations and loading operations here

# Drop Staging Table
def drop_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS loan_staging1 CASCADE;""")


with conn.cursor() as cursor:
    drop_staging_table(cursor)

cur.close()
conn.close()

