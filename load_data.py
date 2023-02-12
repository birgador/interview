import csv

with open('job_graph_matrix.csv', mode ='r')as file:
  # reading the CSV file
  csvFile = csv.reader(file)
 
  # displaying the contents of the CSV file
  for lines in csvFile:
        print(lines)