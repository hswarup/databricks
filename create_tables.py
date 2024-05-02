# Remove previous associations if any


dbutils.fs.rm("dbfs:/user/hive/warehouse/bugs", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/bugtracking", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/progress_status_lookup", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/single_player_pond", True)

dbutils.fs.ls("dbfs:/user/hive/warehouse/")


##########################
# File location and type
#########################
file_type = "csv"

bugs_file_location = "/FileStore/tables/Bugs.csv"
bugtracking_file_location = "/FileStore/tables/BugTracking.csv"
progress_status_file_location = "/FileStore/tables/Progress_Status_Lookup.csv"
singleplayer_pond_file_location = "/FileStore/tables/Single_Player_Pond_Dataset.csv"

####################
# CSV options
###################
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

####################
# Build a Pyspark dataframe by loading data from CSV file
####################
bugs_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(bugs_file_location)

bugtracking_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(bugtracking_file_location)

progress_status_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(progress_status_file_location)

singleplayer_pond_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("escape", "\"") \
  .option("sep", delimiter) \
  .load(singleplayer_pond_file_location)
  
########################
# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.
########################

bugs_permanent_table_name = "bugs"
bugtracking_permanent_table_name = "bugtracking"
progress_status_permanent_table_name = "progress_status_lookup"
singleplayer_pond_permanent_table_name = "Single_player_pond"

# Create as permanent table
bugs_df.write.format("parquet").saveAsTable(bugs_permanent_table_name)
bugtracking_df.write.format("parquet").saveAsTable(bugtracking_permanent_table_name)
progress_status_df.write.format("parquet").saveAsTable(progress_status_permanent_table_name)
singleplayer_pond_df.write.format("parquet").saveAsTable(singleplayer_pond_permanent_table_name)

-------------------------
--SELECT a.projectid,a.bugid,b.* FROM bugs a JOIN bugtracking b ON a.projectid = b.projectid AND a.bugid = b.bugid;

%sql
SELECT schema_of_json('{"mid":"H2HS_5049060562","clock_time":16778069,"type":"GROUND","passer_char":176580,"passer_attr":82,"receiver_char":158023,"start_loc":[100.018615723,0.431148589,-24.751880646],"target_loc":[113.083877563,0.100000001,-2.792277336],"receive_loc":[111.753417969,0.365257323,-6.597126484],"pass_details":["INTO_ATTACKING_THIRD","INTO_CENTRE"],"pass_result_details":["PRESSURE_SUCCESS","COMPLETE"],"touch_part":"LFOOT","pass_diff":0.46,"power_meter":0.29,"pass_rel_angle":-2.08}
');

SELECT --schema_of_json(event_params) 
from_json(event_params,'STRUCT<clock_time: BIGINT, mid: STRING, pass_details: ARRAY<STRING>, pass_diff: DOUBLE, pass_rel_angle: DOUBLE, pass_result_details: ARRAY<STRING>, passer_attr: BIGINT, passer_char: BIGINT, power_meter: DOUBLE, receive_loc: ARRAY<DOUBLE>, receiver_char: BIGINT, start_loc: ARRAY<DOUBLE>, target_loc: ARRAY<DOUBLE>, touch_part: STRING, type: STRING>')
event_params
from Single_Player_Pond_1 where event_step=1347 limit 1


# Shot to score ratio (STSR) is assumed to be a measure of how successful the player is able to convert his goal shoot attempts to actual goals.
# STSR is expressed as a percentage below -

# STSR = Total goals scored *100 / Total shots attempted
%sql

SELECT ROUND(COUNT(1)*100 / (SELECT COUNT(1)+1 FROM single_player_pond WHERE event_name='shot'),2) as STSR FROM single_player_pond WHERE event_name='score'






%sql

-- Pick a sample record from the table where event_name='pass'
-- Pass that sample data from event_params field into "schema_of_json" function to understand the data type of "value" belonging to each "key"
SELECT schema_of_json('{"mid":"H2HS_5049060562","clock_time":16778156,"type":"GROUND","passer_char":168542,"passer_attr":92,"receiver_char":222492,"start_loc":[112.679397583,2.083006859,23.990615845],"target_loc":[111.396255493,0.100000001,57.968891144],"receive_loc":[106.728500366,0.756773651,52.239437103],"pass_details":["FIRST_TOUCH"],"pass_result_details":["GOOD","COMPLETE"],"touch_part":"RFOOT","pass_diff":2.4,"power_meter":0.28,"pass_rel_angle":0.51}
') as event_param_schema;

%sql

-- Use the above information to create a Temporary view so as to easily query the embedded keys and values within event_params field
-- As an alternative , a Table could have been created instead of a view.
-- Considering this is a one-off analysis and to save storage space ,I went with the temporary view option.

CREATE OR REPLACE TEMP VIEW single_player_pond_parsed AS
SELECT player_id,event_name,event_step,
from_json(event_params,'STRUCT<clock_time: BIGINT, mid: STRING, pass_details: ARRAY<STRING>, pass_diff: DOUBLE, pass_rel_angle: DOUBLE, pass_result_details: ARRAY<STRING>, passer_attr: BIGINT, passer_char: BIGINT, power_meter: DOUBLE, receive_loc: ARRAY<DOUBLE>, receiver_char: BIGINT, start_loc: ARRAY<DOUBLE>, target_loc: ARRAY<DOUBLE>, touch_part: STRING, type: STRING>') as 
event_params
from single_player_pond WHERE event_name='pass'


%sql

DESCRIBE single_player_pond_parsed
-- As can be seen below , event_params is now a STRUCT data type instead of a STRING
-- Thus helps to query the individual elements within it

%sql

--Lets have a look at the transformed data(though just a data type change) in our temporary view
SELECT * FROM single_player_pond_parsed


%sql
-- Below query gets the no. of occurences/records for each "shot_type"
-- It is evident that "GROUND" shot type is the clear winner

SELECT COUNT(1) occurences ,event_params.type shot_type FROM single_player_pond_parsed GROUP BY event_params.type ORDER BY 1 DESC


# Load into a dataframe to use seaborn python library for visualization
shot_type_pysparkDF=spark.sql('SELECT event_params.type AS shot_type,event_params.touch_part  FROM single_player_pond_parsed ')
shot_type_pandasDF = shot_type_pysparkDF.toPandas()


# seaborn python library is used for visualization
import seaborn as sns
from matplotlib import rcParams

# "Histogram" is the chart type used to understand the distribution/frequency of shot_types by the player
# The hue/color indicates the body part used to play that shot as well

# The below Viz. shows that -
# "GROUND" is the highest shot_type used by the player
# "BOTHHAND" was used only when THROWING" (which seem to make sense and provide confidence that the data is clean)

rcParams['figure.figsize'] = 15,10
sns.histplot(data=shot_type_pandasDF,y="shot_type",hue="touch_part",multiple="stack",legend=True)








--a)	Please provide the SQL query that would accurately join the provided tables: ‘Bugs’, ‘BugTracking’, and ‘ProgressStatusLookup’

%sql
CREATE OR REPLACE TEMPORARY VIEW bugs_complete_view AS 
SELECT  
   bugs.*
  ,bugtracking.* EXCEPT(bugtracking.projectid, bugtracking.bugid, bugtracking.`game area`)  
                    -- Do not print projectid, bugid from bugtracking(as these are already printed from bugs)
  ,progress_status_lookup.progressstatusname
FROM 
  bugs 
LEFT OUTER JOIN bugtracking 
    ON bugs.projectid = bugtracking.projectid AND bugs.bugid = bugtracking.bugid 
    -- Used LEFT OUTER JOIN to be on the safer side(Could have used JOIN instead as bugtracking has a corresponding record for every bugid from bugs) 
LEFT OUTER JOIN progress_status_lookup   
    -- Definitely have to use LEFT OUTER JOIN here as progressstatusname is not defined for few progress status IDs
    ON bugs.projectid = progress_status_lookup.projectid AND bugtracking.`progress status id`=progress_status_lookup.progressstatusid
    
--(d)
--Within the "PR - Problem Report" Issue Type in the table, how many null Priority values are there?
%sql

SELECT COUNT(1) FROM bugs WHERE issuetype='PR - Problem Report' AND priority = 'NULL'


### (c) How many distinct bugs have a null value in either the ProgressStatusName or Priority field in the joined table? 

### What could be the possible causes of this?
### Answer : (a) ProgressStatusName - It appears that whenever a bug is opened , There is a record inserted in bugtracking with progresstatusid as Blank or NULL.
###          Considering the timestamp of this NULL record is same as that of the next record with status as 'Confirmed' , It may be OK to ignore the NULL record ###          for any analytics.

###          (b) Priority - The reason this field has NULL values is probably due to the tester not assigning a priority intentionally(due to him/her not being aware of the right priority to assign) or unintentionally(due to priority not being a mandatory field in the bug tracking tool)

%sql

SELECT COUNT(DISTINCT projectid,bugid) FROM bugs_complete_view WHERE progressstatusname IS NULL OR priority = 'NULL'


--####  iii. Which Test Team found the most Severity = "B" bugs, in the Bugs table?

%sql

SELECT * FROM 
(
    SELECT 
      testteam,
      COUNT(1) severity_c_bugs_found 
    FROM 
      bugs 
    WHERE
      severity='C'
    GROUP BY 
      testteam 
    ORDER BY 
      2 DESC           -- Order by bugs found in descending order
) 
LIMIT 1     -- Pick only the first record which has found the most bugs with severity=C 


### (b)	How many distinct bugs with an ‘In Progress’ progress status exist in this joined table?
%sql

SELECT COUNT(DISTINCT bugid) FROM bugs_complete_view WHERE progressstatusname='In Progress'

### i. What is the average Open Bug Life for each Severity Level?
%sql

SELECT 
  severity,
  ROUND(AVG(bug_open_days),2) AS Average_Open_Bug_Life
FROM
(
  SELECT *,
  datediff (to_timestamp(assignedtime_local, 'h:m a M-d-yyyy') ,to_timestamp(createdtime_local, 'h:m a M-d-yyyy')) AS bug_open_days 
  FROM bugs
)
GROUP BY
  severity
ORDER BY
  severity
  
  OR
  
  
  
%sql

SELECT 
  severity,
  COUNT(1) AS No_of_bugs,
  AVG(bug_open_days) AS Average_Bug_Open_Life
FROM
  (
    SELECT 
      projectid,
      bugid,
      severity, 
      MIN(to_timestamp(dateassigned, 'h:m a M-d-yyyy')) AS bug_opened_time,
      MAX(to_timestamp(dateassigned, 'h:m a M-d-yyyy')) AS bug_closed_time,
      datediff (MAX(to_timestamp(dateassigned, 'h:m a M-d-yyyy')) ,MIN(to_timestamp(dateassigned, 'h:m a M-d-yyyy')) ) AS bug_open_days
    FROM bugs_complete_view 
    WHERE progressstatusname='Confirmed' OR progressstatusname LIKE '%Closed%'
    GROUP BY
      projectid,bugid,severity
  )
GROUP BY
  severity
ORDER BY 
  severity
  
### KPI 3: Bug Find – The number of bugs found over time
### •	What business value do you think this KPI could provide?
### •	Please create a running total visualization depicting number of bugs found over time.

%sql

SELECT 
  SUM(Bugs_Found) OVER (ORDER BY bug_created_time) as Running_Total_Bugs_Found,
  bug_created_time
FROM
(
  SELECT 
    COUNT(1) as Bugs_Found, 
    TO_DATE(to_timestamp(createdtime_local, 'h:m a M-d-yyyy'),'M-d-yyyy') AS bug_created_time 
  FROM 
    bugs 
  GROUP BY
    TO_DATE(to_timestamp(createdtime_local, 'h:m a M-d-yyyy'),'M-d-yyyy') 
)
ORDER BY 
  bug_created_time

  