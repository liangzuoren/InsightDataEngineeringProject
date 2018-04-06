# InsightDataEngineeringProject
Coding Challenge for Insight Data Engineering (EDGAR Analytics)

This shell will run a Python program to analyze EDGAR weblogs off of the SEC website.
The program aims to provide an efficient way to analyze the massive amount of data, prioritizing
time complexity and space complexity in its design. The program allows for scalability with large
datasets. The pipeline takes in two files as input, a file with the weblogs and another file to 
indicate the amount of time that a user can be inactive prior to discounting a session. The output 
file will be a log of all the sessions that occurred in the weblog file in time-sorted order.

Inputs: inactivity_period.txt (Text file for inactivity period), log.csv(weblog file)
Outputs: sessionization.txt (Text file describing all the sessions in the weblog)

Dependencies required: Python 3.6 or higher
Packages used are default packages included within the base python installation

