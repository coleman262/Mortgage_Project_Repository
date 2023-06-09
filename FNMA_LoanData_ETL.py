import psycopg2
import pandas as pd
import numpy as np

# AWS Pycharm Postgres Connection Query for ETL

hostname = 'fnmaloanhist.ccsgwcueebaq.us-east-2.rds.amazonaws.com'
database = 'postgres'
username = 'MaxDataEx'
pwd = 'balIbear0129!'

conn = psycopg2.connect(
    host=hostname,
    database=database,
    user=username,
    password=pwd
)

cur = conn.cursor()
conn.autocommit = True

####Extract Data####



dataDF = ('C:/Users/colem/PycharmProjects/pythonProject2/2022Q3.csv')


def create_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS loan_staging CASCADE;
    CREATE UNLOGGED TABLE loan_staging(
    
  Pool varchar(25) DEFAULT NULL,
  LoanID bigint DEFAULT NULL,
  Period int DEFAULT NULL,
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
  ZeroBalDt int DEFAULT NULL,
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
  ZeroBalChgDt int DEFAULT NULL,
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
    

    );""")


with conn.cursor() as cursor:
    create_staging_table(cursor)


def send_csv_to_psql(connection, csv, table_):
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '|'"
    file = open(csv, "r")
    table = table_
    with connection.cursor() as cur:
        cur.execute("truncate " + table + ";")  # to eliminate duplicates
        cur.copy_expert(sql=sql % table, file=file)
        connection.commit()

    return connection.commit()


send_csv_to_psql(conn, dataDF, 'loan_staging')



####LOAD###
def data_transform_load(cursor):
    cursor.execute("""
    INSERT INTO fnma_loanhist (
PK,
LoanID,
Period,
Channel,
Seller,
Servicer,
MasterServicer,
OrigRate,
CurRate,
OrigUPB,
UPBIssue,
CurUPB,
OrigTerm,
OrigDate,
FstPayDt,
Age,
RemLegalTerm,
RemTerm,
MatDate,
OrigLTV,
OrigCLTV,
Borrowers,
DTI,
CreditScoreOrig,
CoCreditScoreOrig,
FstTimeHomeBuy,
Purpose,
PropType,
Units,
Occupancy,
State,
MSA,
Zip,
MiIns,
AmortType,
PrepayPen,
IO,
IOPrinDT,
IOAmortTerm,
DlqStatus,
Paystring,
ModFlag,
MtgencCaclInsur,
ZeroBalCode,
ZeroBalDt,
UPBLiq,
RepurchDt,
SchedPrinCur,
TotPrinCur,
UnschedPrinCur,
LastPaidInstalDt,
FC_Date,
DispDt,
FC_Costs,
PropPrevCost,
AssetRecovCost,
MiscHoldingExpCredit,
TaxesHoldProp,
NetSalesProceed,
CreditEnhanceProceeds,
RepurchWholeProceed,
OtherFCProceeds,
ModNonIntUPB,
PrincForgiveAmt,
OrigListStartDt,
OrigListPx,
CurListStartDt,
CurListPx,
CreditScoreIss,
CoCreditScoreIss,
CreditScoreCur,
CoCreditScoreCur,
MtgeInsureType,
ServActivity,
CurPerModLoss,
CumPerModLoss,
CurCreditEventLoss,
CumCreditEventLoss,
SpeciaEligProgram,
FCPrinWriteOff,
ReloMtge,
ZeroBalChgDt,
LoanHoldback,
LoanHoldbackDT,
DlqAccurInt,
PropvalMethod,
HighBal,
ARMPeriod,
ArmProduct,
FstPerCap,
PerCapAdjFreq,
PerCap,
NextPayChngDt,
Index,
ArmCapStrct,
FstIntRateCapPerc,
PerIntRateCapPerc,
LifeCap,
MtgeMargin,
BalloonInd,
ARMPlan,
BorrowerAsstPlan,
HLTVRefiOption,
DealName,
RepurchMakeWhole,
AltDelResol,
AltDelResCnt,
DeferralAmt)

(Select

Pool,
LoanID,
Period,
Channel,

CASE 
    When Seller like 'Amtrust%' then 'Amtrust'
    When Seller like 'JPMorgan%' then 'JPMorgan'
    When Seller like 'Bank of America%' then 'BOA' 
    When Seller like 'Better%' then 'Better' 
    When Seller like 'Bishops%' then 'Bishops' 
    When Seller like 'Citi%' then 'Citi' 
    When Seller like 'First Tennesse%' then 'FTN' 
    When Seller like 'Flagstar%' then 'Flagstar' 
    When Seller like 'Ge Mortgage%' then 'GMAC' 
    When Seller like 'loanDepot%' then 'loanDepot' 
    When Seller like '%New American%' then 'NewAmFund' 
    When Seller like 'PNC%' then 'PNC' 
    When Seller like 'Penny%' then 'Pennymac' 
    When Seller like 'RBC%' then 'RBC' 
    When Seller like 'Regions%' then 'Regions' 
    When Seller like 'Suntrust%' then 'Truist' 
    When Seller like 'USAA%' then 'USAA' 
    When Seller like 'Usaa%' then 'USAA' 
    When Seller like 'Wells%' then 'Wells'  
    Else 'Other'
END as Seller, 

CASE 
    When Servicer like 'Amtrust%' then 'Amtrust'
    When Servicer like 'JPMorgan%' then 'JPMorgan'
    When Servicer like 'Bank of America%' then 'BOA' 
    When Servicer like 'Better%' then 'Better' 
    When Servicer like 'Bishops%' then 'Bishops' 
    When Servicer like 'Citi%' then 'Citi' 
    When Servicer like 'First Tennesse%' then 'FTN' 
    When Servicer like 'Flagstar%' then 'Flagstar' 
    When Servicer like 'Ge Mortgage%' then 'GMAC' 
    When Servicer like 'GMAC%' then 'GMAC' 
    When Servicer like 'loanDepot%' then 'loanDepot' 
    When Servicer like 'PNC%' then 'PNC' 
    When Servicer like 'Penny%' then 'Pennymac' 
    When Servicer like 'RBC%' then 'RBC' 
    When Servicer like 'Regions%' then 'Regions' 
    When Servicer like 'Suntrust%' then 'Truist' 
    When Servicer like 'USAA%' then 'USAA' 
    When Servicer like 'Usaa%' then 'USAA' 
    When Servicer like 'Wells%' then 'Wells' 
    Else 'Other'
END as Servicer, 
MasterServicer,
OrigRate/100,
CurRate/100,
round(OrigUPB,2),
round(UPBIssue,2),
round(CurUPB,2),
OrigTerm,
OrigDate,
FstPayDt,
Age,
RemLegalTerm,
RemTerm,
MatDate,
OrigLTV/100,
OrigCLTV/100,
Borrowers,
DTI/100,
CreditScoreOrig,
CoCreditScoreOrig,
FstTimeHomeBuy,
Purpose,
PropType,
Units,
Occupancy,
State,
MSA,
Zip,
MiIns,
AmortType,
PrepayPen,
IO,
IOPrinDT,
IOAmortTerm,
DlqStatus,
Paystring,
ModFlag,
MtgencCaclInsur,
ZeroBalCode,
ZeroBalDt,
round(UPBLiq,2),
RepurchDt,
round(SchedPrinCur,2),
round(TotPrinCur,2),
round(UnschedPrinCur,2),
LastPaidInstalDt,
FC_Date,
DispDt,
FC_Costs,
round(PropPrevCost,2),
round(AssetRecovCost,2),
round(MiscHoldingExpCredit,2),
TaxesHoldProp,
round(NetSalesProceed,2),
round(CreditEnhanceProceeds,2),
round(RepurchWholeProceed,2),
round(OtherFCProceeds,2),
round(ModNonIntUPB,2),
round(PrincForgiveAmt,2),
OrigListStartDt,
OrigListPx,
CurListStartDt,
CurListPx,
CreditScoreIss,
CoCreditScoreIss,
CreditScoreCur,
CoCreditScoreCur,
MtgeInsureType,
ServActivity,
CurPerModLoss,
CumPerModLoss,
round(CurCreditEventLoss,2),
round(CumCreditEventLoss,2),
SpeciaEligProgram,
FCPrinWriteOff,
ReloMtge,
ZeroBalChgDt,
LoanHoldback,
LoanHoldbackDT,
DlqAccurInt,
PropvalMethod,
HighBal,
ARMPeriod,
ArmProduct,
FstPerCap,
PerCapAdjFreq,
PerCap,
NextPayChngDt,
Index,
ArmCapStrct,
FstIntRateCapPerc,
PerIntRateCapPerc,
LifeCap,
MtgeMargin,
BalloonInd,
ARMPlan,
BorrowerAsstPlan,
HLTVRefiOption,
DealName,
RepurchMakeWhole,
AltDelResol,
AltDelResCnt,
round(DeferralAmt,2)


    from loan_staging
Where age >=0
    );""")

with conn.cursor() as cursor:
    data_transform_load(cursor)



def drop_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS loan_staging CASCADE;""")

with conn.cursor() as cursor:
    drop_staging_table(cursor)


def update_pk(cursor):
    cursor.execute("""
    UPDATE fnma_loanhist
Set pk =
CASE When (length(period) = 5) THEN (concat(loanid, right(period, 4), 0, left(period,1)))
ELSE concat(loanid, right(period, 4), left(period,2))
END;""")
with conn.cursor() as cursor:
    update_pk(cursor)



def update_period(cursor):
    cursor.execute("""
UPDATE fnma_loanhist
Set period = right(pk,6);""")

with conn.cursor() as cursor:
    update_period(cursor)


cur.close()
conn.close()

