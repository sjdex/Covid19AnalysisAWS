import redshift_connector

conn = redshift_connector.connect(
    host='redshift-cluster-1.cqd5znshhjvl.ap-south-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password='Password1'
)

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
copy dimDate from 's3://dexter-covid-project/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::136178993549:role/redshift-s3'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy dimHospital from 's3://dexter-covid-project/output/dimHospital.csv'
credentials 'aws_iam_role=arn:aws:iam::136178993549:role/redshift-s3'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy dimRegion from 's3://dexter-covid-project/output/dimRegion.csv'
credentials 'aws_iam_role=arn:aws:iam::136178993549:role/redshift-s3'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy factCovid from 's3://dexter-covid-project/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::136178993549:role/redshift-s3'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")