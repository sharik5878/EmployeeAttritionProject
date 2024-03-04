-- Databricks notebook source
-- MAGIC %md
-- MAGIC Challenges stemming from employee attrition pose significant concerns for organizations, as employees form the foundation of any successful enterprise. The consequences of high turnover rates include substantial investments of both time and money in training new hires, the loss of invaluable experience, decreased productivity, and ultimately, diminished profits.
-- MAGIC
-- MAGIC In addressing this issue, pertinent question emerge:
-- MAGIC
-- MAGIC What are the primary factors driving employee attrition within the organization?
-- MAGIC

-- COMMAND ----------

select * from employee_attrition

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Total Number of Employees

-- COMMAND ----------


select sum(EmployeeCount) as Total_Employees from employee_attrition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Employee Count Division based on attrition

-- COMMAND ----------

select sum(EmployeeCount), Attrition from employee_attrition
group by Attrition 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC AGE Analysis -- At which particular age range attrition is high (18-24, 25-31,32-38,39-45,46-52,52+)

-- COMMAND ----------

select sum(EmployeeCount),
case when age between 20 and 25 then '20-25' when age between 26 and 32 then '26-32' when age between 33 and 40 then '33-40' when age between 41 and 50 then '41-50'else '50+' end age_group
from employee_attrition
where attrition='Yes'
group by 2
order by 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Department Analysis - in which department Attrition is high

-- COMMAND ----------

select sum(EmployeeCount), department from employee_attrition
where Attrition= 'Yes'
group by department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Level of education Analysis - ( 1- below college, 2 - college, 3 bachelor , 4 master, 5 doctor)

-- COMMAND ----------

select sum(EmployeeCount),
case when education=1 then 'below college' when education=2 then ' college' when education=3 then 'bachelor' when education=4 then 'master' else 'doctor' end ed_goup
from employee_attrition
where attrition='Yes'
group by 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Environment Satisfaction Analysis - (1-low, 2-medium,3-high,4 highly satisfied)

-- COMMAND ----------

select sum(EmployeeCount),
EnvironmentSatisfaction
from employee_attrition
where attrition='Yes'
group by 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Business travel Analysis

-- COMMAND ----------


select sum(EmployeeCount),
BusinessTravel
from employee_attrition
where attrition='Yes'
group by 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Insights that are revealed in this analysis through spark sql are:
-- MAGIC 1. employees whose age range is between 26-32 are the most leaving the organization.
-- MAGIC 2. R&D department people are leaving from org.
-- MAGIC 3. 41.8% people having bachelor degree left org
-- MAGIC 4. 72 employees not satisfied with environment left org
-- MAGIC 5. 156 employees who were travelling rarely left org
-- MAGIC
-- MAGIC Based on these reasons of attrition the required actions need to address the issues.
