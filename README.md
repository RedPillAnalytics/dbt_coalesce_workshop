**2022 dbt coalesce project: Detecting Data Anomalies via DBT Inspection Layer**

[Project Details Gdrive](https://drive.google.com/drive/folders/1jiaXGYHKTrZtNyt-YfKaFh8G3AlPcilv?usp=sharing)

This is a dbt native method to handle incoming data issues without the need to bring in other tools to help profile, validate, cleanse, and/or observe. Those tools are great, and recommended, but often have larger barriers of entry and this solution can provide a lighter approach to the problem.

**Examples of Data Problems**
When we call say data anomalies, some examples that we look for include:
* Null columns requiring a value
* Duplicate data for the same business key
* Incorrectly formatted data such as phone numbers, zip codes, etc.

**The Data Set and Flow**
To be able to effectively showcase the problem we first need to establish a dbt project and some data to look at. For this, we’re utilizing data about school districts and the method of teaching they utilized for a school year from healthdata.gov. In our demonstration we are creating some scenarios for the data pipeline. They include:
* Files contain weekly data for each district but will be delivered once per quarter
* Files are delivered to an SFTP server in CSV format
* Files will be loaded by Fivetran into Snowflake

**dbt Project Setup**
We follow the dbt Lab's best practices to structure the project.
1. Staging from sources
2. Intermediate models
3. Marts of analytical models

Inspection Layer Models

We used dbt native methods to implement an inspection layer to ensure erroneous data sets can be flagged and quarantined while the rest can load uninterrupted.

We materialized our tests as tables to create an inspection model layer. 

Before inspection layer we had the following dbt model layers:
`sources->staging->intermediate->marts`

With inspection we are adding inspection after staging which looks like:
`sources->staging->inspection->intermediate->marts`

**Here’s What’s Good About It**

Isolates client files with issues while allowing the rest of client files to flow through to target(s).
Materialized tables track which client had which DQ issue by file and line number.
Can quickly identify which clients have what problems and which files need re-submission.




### dbt Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
