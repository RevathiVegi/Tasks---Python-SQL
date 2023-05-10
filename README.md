# Tasks---Python-SQL
## Task 1- Extract data from an API. Convert the .json format into pandas data frame. Inserting the data frame into the corresponding tables in the database. 
```Python
from sqlalchemy import create_engine,inspect
import pandas as pd
engine = create_engine('postgresql+psycopg2://user_cab5a8b57df04c669feed9bcb7fefa5a:9d6644228f874ddfa397f5b2ceea660b@3.71.166.33/db_ed32a5d6a7ca4b198ddde5fb839659ab')
connection = engine.connect()

connection.__dict__

df=pd.read_sql("SELECT * FROM posts",connection)

df

import requests

url="https://jsonplaceholder.typicode.com/posts"

posts_response=requests.get(url)

posts=posts_response.json()

posts_df=pd.DataFrame(posts)

posts_df.head()

posts_df.to_sql('posts', con=engine, if_exists='append',index=False)

posts_df.head(1)

columns=posts_df.columns.to_list()

posts_df_1=posts_df.rename(columns={"userId": "user_id"})

posts_df_1.to_sql('posts', con=engine, if_exists='append',index=False)


comments_url= "https://jsonplaceholder.typicode.com/comments"

comments_response=requests.get(comments_url)

comments=comments_response.json()

comments_df=pd.DataFrame(comments)


comments_df_1=comments_df.rename(columns={"postId": "post_id"})
comments_df_1.head()

comments_df_1.to_sql('post_comments', con=engine, if_exists='append',index=False)
```
This code is designed to extract data from a web-based API and store it in a PostgreSQL database. It does this by converting the data from the API, which is in JSON format, into a pandas DataFrame, and then inserting the DataFrame into the corresponding tables in the database.

## Prerequisites
To run this code, you will need the following installed:

1) Python 3.x
2) SQLAlchemy
3) pandas
4) psycopg2
5) requests
- You will also need access to a PostgreSQL database with the appropriate credentials.
# How to Use the Code
## Import the necessary libraries:
```Python
from sqlalchemy import create_engine,inspect
import pandas as pd
import requests
```
## Create a connection to the PostgreSQL database using SQLAlchemy:
```Python
engine = create_engine('postgresql+psycopg2://username:password@hostname/database')
connection = engine.connect()
```
Replace "username", "password", "hostname", and "database" with your own values.

## Extract data from the API by making a request to the API endpoint using requests:
```Python
url="https://jsonplaceholder.typicode.com/posts"
posts_response=requests.get(url)
posts=posts_response.json()
posts_df=pd.DataFrame(posts)
```
Replace "url" with the URL of the API endpoint you want to extract data from.

## Insert the data into the "posts" table in the database:
```Python
posts_df.to_sql('posts', con=engine, if_exists='append',index=False)
```
## Extract data from another API and insert it into another table in the database:
```Python
comments_url= "https://jsonplaceholder.typicode.com/comments"
comments_response=requests.get(comments_url)
comments=comments_response.json()
comments_df=pd.DataFrame(comments)
comments_df_1=comments_df.rename(columns={"postId": "post_id"})
comments_df_1.to_sql('post_comments', con=engine, if_exists='append',index=False)
```
Replace "comments_url" with the URL of the second API endpoint you want to extract data from, and replace "post_comments" with the name of the table you want to insert the data into.
## Conclusion
This code provides a simple way to extract data from an API and store it in a PostgreSQL database. By following the steps outlined above, you can use this code to extract data from any API and insert it into any PostgreSQL database with the appropriate credentials.

## TASK 2- Excel Data ingestion to PostGreSQL database and SQL analysis of Baseball dataset.
# To Ingest Data to PostGreSQL
## Step 1: Import the necessary libraries
```Python
import pandas as pd
import requests
from sqlalchemy import create_engine
```
- In this step, we import the necessary libraries that we will use throughout the code. pandas is a popular library used for data manipulation and analysis.    
  requests is a library used to send HTTP requests. sqlalchemy is a library used to work with databases.
## Step 2: Connect to the Google Sheets API
```Python
endpoint = "https://docs.google.com/spreadsheets/d"
params = {'q':'mimeType="application/vnd.ms-excel"','fields':'files(1CwH3U0ePFU2PE_7jv5eEX6P1rTFvAwyY,webContentLink)'}
api_key = 'b7c8287d-8963-427d-b991-4d882790773e'

response = requests.get(endpoint, params=params, headers={'Authorization':f'Bearer {api_key}'})
```
- In this step, we connect to the Google Sheets API using the requests library. 
- We specify the endpoint URL and set the query parameters, which include the MIME type and the fields we want to retrieve. We also pass our API key in the 
  headers to authenticate the request.
## Step 3: Read the data from the Excel sheet
```Python
location="Task3_Baseball_Dataset.xlsx"

df=pd.read_excel(location, sheet_name=[3,4])

managers=df[3]

awards_managers=df[4]
```
- In this step, we read the data from the Excel sheet using the pandas library's read_excel() function. 
- We specify the location of the Excel sheet and the sheet names we want to read. 
- In this case, we are reading the third and fourth sheets from the Excel file and assigning them to managers and awards_managers data frames respectively.
## Step 4: Create a table in the PostgreSQL database
```Python
managers_table="""
CREATE TABLE IF NOT EXISTS managers (
  id SERIAL,
  player_id VARCHAR(128),
  year_id int,
  team_id VARCHAR(50),
  lg_id  VARCHAR(50),
  in_season int,
  g int,
  w int,
  l int,
  rank float,
  plyr_mgr VARCHAR(1),
  PRIMARY KEY (id)
)
"""

engine = create_engine('postgresql+psycopg2://postgres:postgres@database/sample')
connection = engine.connect()

result=connection.execute(managers_table)
```
- In this step, we create a table in the PostgreSQL database using SQL syntax. 
- We specify the table's column names, data types, and primary key. 
- We then create a connection to the database using the create_engine() function from the sqlalchemy library. 
- We pass the connection string with the username, password, and database name. 
- Finally, we execute the SQL statement using the execute() function from the connection object.
## Step 5: Insert data into the PostgreSQL database
```Python
managers.to_sql('managers', con=engine, if_exists='append',index=False)

managers_df_1=managers.rename(columns={"playerID": "player_id","lg_ID":"lg_id", "G":"g", "W":"w", "L":"l", "plyrMgr":"plyr_mgr"})

managers_df_1.to_sql('managers', con=engine, if_exists='append',index=False)

awards_managers_df_1=awards_managers
```
# SQL Analysis of Baseball Dataset ingested (with the above code).
## Question1: Of the teams who have won atleast 70 games in the year 2010, how many of them have changed at least their managers once during the same year, and what is the teamID for each team?
The code is selecting data from a table called "managers" and counting the number of teams that meet certain criteria, specifically:
1) The data is filtered to include only records for the year 2010 using a WHERE clause.
2) The data is grouped by the "team_id" column.
3) The groups are filtered to include only those with a sum of "W" (number of wins) greater than or equal to  
   70 and a count of "player_id" greater than 1.
4) The resulting teams are concatenated into a string with a count of the number of teams and a comma-
   separated list of the team IDs.
## Here is a breakdown of the SQL code:
```Python
WITH T AS
(SELECT TEAM_ID,
		SUM(W) WINS ,
		COUNT(PLAYER_ID) CNT
	FROM PUBLIC.MANAGERS
	WHERE YEAR_ID = '2010'
	GROUP BY TEAM_ID
	HAVING SUM(W) >= 70
	AND COUNT(PLAYER_ID) > 1)

SELECT CONCAT(COUNT(TEAM_ID),' teams: ',STRING_AGG(TEAM_ID,', ')) TEAMS
FROM T.
```
- The code starts with a common table expression (CTE) called "T". 
- This CTE selects the "team_id", the sum of "W" (wins), and the count of "player_id" for all records in the "managers" table that have a "year_id" value of  
  '2010'. 
- The results are grouped by "team_id" and then filtered using a HAVING clause to include only those groups with a sum of "W" greater than or equal to 70 and a
  count of "player_id" greater than 1.
- The second part of the code selects from the CTE "T". 
- It uses the CONCAT function to combine the count of teams meeting the criteria with a string containing the team IDs separated by commas. 
- The resulting string is returned as the column "TEAMS".
## Here is an example of how to use this code:
- Make sure that you have a PostgreSQL database with a table called "managers" that contains the relevant data. You can use the code in the original post to  
  create the table and insert data.
- Open a SQL client such as pgAdmin or psql.
- Copy the SQL code into the client and modify the "YEAR_ID" value as needed.
- Execute the SQL code.
- The results will be a count of the number of teams meeting the criteria and a comma-separated list of the team IDs.
## Output of the above code
```Python
2 teams: FLO, CHN
```
## Question2: What are the playedIDs for each teamID mentioned above? Let's say there are 3 teams, and each team changes their managers 3x, then there are 3*3. Separate the answer by ":" and ",".
## Sample Answer :
KCA : coxbo01, trembda99, francte01

LAA : yostne01, bakerdu01, tracyji01

HOU : millsbr01, gardero01, manueje01
- This SQL code is designed to analyze a baseball dataset, specifically the performance of     
  managers and teams during the year 2010. 
- The purpose of this code is to identify the teams that had 70 or more wins and more than one
  player on their roster in the given year, and to display the team ID along with the player 
  IDs for those teams.
## Code Breakdown
The SQL code is divided into three subqueries:
## Subquery 1: WINS
```Python
WITH WINS AS
	(SELECT TEAM_ID,
			SUM(W) WINS ,
			COUNT(PLAYER_ID) CNT
		FROM PUBLIC.MANAGERS
		WHERE YEAR_ID = '2010'
		GROUP BY TEAM_ID
		HAVING SUM(W) >= 70
		AND COUNT(PLAYER_ID) > 1)
```
- This subquery creates a temporary table called WINS which aggregates the number of wins and 
  player count for each team during the year 2010. 
- The HAVING clause filters out teams with less than 70 wins and only one player on their 
  roster.
## Subquery 2: TEAMS
```Python
	TEAMS AS
	(SELECT PM.TEAM_ID,
			PLAYER_ID
		FROM PUBLIC.MANAGERS PM
		JOIN WINS W ON PM.TEAM_ID = W.TEAM_ID
		WHERE YEAR_ID = '2010' )
```
- This subquery creates another temporary table called TEAMS which lists the team ID and player ID for each player on a team that met the criteria established in the WINS subquery.
## Subquery 3: OP
```Python
	OP AS
	(SELECT TEAM_ID,
			STRING_AGG(PLAYER_ID,', ') PLAYER_IDS
		FROM TEAMS
		GROUP BY TEAM_ID)
```
- This subquery creates a final temporary table called OP which concatenates all player IDs for each team into a comma-separated string, grouped by team ID.
## Main Query
```Python
SELECT CONCAT(TEAM_ID,' : ',PLAYER_IDS)
FROM OP
```
- The main query selects the team ID and concatenated player IDs for each team in the OP 
  temporary table, separated by a colon and a space. 
- This displays the final output that satisfies the requirements of the analysis.
## Output of the above code
```Python
CHN : pinielo01, quademi99
FLO : gonzafr99, rodried02
```
## Question3: Please rank each playerID of each team mentioned above by descending order and pick only the latest one, so that each team only has one playerID left.
## Code Breakdown
The SQL code is divided into two subqueries and one main query:
## Subquery 1: WINS
```Python
WITH WINS AS
	(SELECT TEAM_ID,
			SUM(W) WINS ,
			COUNT(PLAYER_ID) CNT
		FROM PUBLIC.MANAGERS
		WHERE YEAR_ID = '2010'
		GROUP BY TEAM_ID
		HAVING SUM(W) >= 70
		AND COUNT(PLAYER_ID) > 1)
```
- This subquery creates a temporary table called WINS which aggregates the number of wins and
  player count for each team during the year 2010. 
- The HAVING clause filters out teams with less than 70 wins and only one player on their 
  roster.
## Subquery 2: RNK
```Python
	RNK AS
	(SELECT PM.TEAM_ID,
			RANK() OVER (PARTITION BY PM.TEAM_ID ORDER BY PLAYER_ID DESC) RNK,
			PM.PLAYER_ID
		FROM PUBLIC.MANAGERS PM
		JOIN WINS W ON PM.TEAM_ID = W.TEAM_ID
		WHERE YEAR_ID = '2010' )
```
- This subquery creates another temporary table called RNK which lists the team ID, player ID,   and rank for each player on a team that met the criteria established in the WINS subquery.
- The RANK function assigns a rank to each player based on their player ID in descending order 
  within their respective team.
## Main Query
```Python
SELECT TEAM_ID,
	PLAYER_ID
FROM RNK
WHERE RNK = 1.
```
- The main query selects the team ID and player ID for each team in the RNK temporary table 
  where the rank is equal to 1. 
- This displays the final output that satisfies the requirements of the analysis.
## Output of the above code
```Python
"team_id","player_id"
"CHN","quademi99"
"FLO","rodried02"
```
- In conclusion, this SQL code is a useful tool for analyzing a baseball dataset and identifying the team with the highest-ranked player for each team that met  
  the specified criteria. 
- By creating temporary tables and utilizing SQL functions, the code efficiently filters and ranks the data to produce a clear and concise output.
- This documentation provides a breakdown of the code and its purpose, making it easy for anyone to understand and use in their own analysis of the dataset. 


