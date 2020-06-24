import boto3
import gzip
import time
import datetime
import os

#This function creates an S3 bucket to store flow log records. It accepts three arguments:
#client - a low-level boto3 client representing Amazon Simple Storage Service
#BUCKET_NAME - the name of the bucket to be created. Consists of the ID of the VPC being monitored and "-flow-log-storage"
#REGION - the region in which to create the bucket. Must be the same as the AWS Default Region in the AWS CLI config file.

def create_s3_bucket(client, BUCKET_NAME, REGION):
  location = {'LocationConstraint': REGION}
  response = client.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration=location)
  print("Created bucket successfully")

#This function first deletes the contents of an S3 bucket, and then the bucket itself. It is called when the program execution is terminated via
#a keyboard interrupt. It accepts three arguments:
#BUCKET_NAME - the name of the bucket to delete.
#CLIENT - a low-level boto3 client representing Amazon Simple Storage Service
#RESOURCE - a boto3 resource representing Amazon Simple Storage Service. 

def delete_s3_bucket(BUCKET_NAME, CLIENT, RESOURCE):
  bucket = RESOURCE.Bucket(BUCKET_NAME)
  bucket.objects.all().delete()
  response = CLIENT.delete_bucket(Bucket=BUCKET_NAME)
  print("Deleted bucket successfully")

#This function sets up a VPC Flow Log to monitor an AWS EC2 instance. It returns the flow log ID of the flow log created, if successful.
#It accepts three arguments:
#client - a low-level boto3 client representing the Amazon Elastic Compute Cloud.
#VPC_ID - the ID of the VPC to create Flow Logs for.
#BUCKET_ARN - the ARN (Amazon Resource Name) of the bucket in which to store Flow Log files. 

def create_vpc_flow_log(client, VPC_ID, BUCKET_ARN):
  print("Creating flow logs...")
  response = client.create_flow_logs(ResourceIds=[VPC_ID], ResourceType='VPC', TrafficType='ALL', LogDestinationType='s3', LogDestination=BUCKET_ARN, MaxAggregationInterval=60);
  print("Created flow logs successfully")
  flow_log_id = response['FlowLogIds'][0]
  print("Flow log ID: " + flow_log_id)
  return flow_log_id

#This function deletes a VPC Flow Log. It is called when the program execution is terminated via a keyboard interrupt.
#It accepts two arguments:
#client - a low-level boto3 client representing the Amazon Elastic Compute Cloud.
#flow_log_id - the ID of the flow log to delete.

def delete_vpc_flow_log(client, flow_log_id):
  print("Deleting flow logs...")
  response = client.delete_flow_logs(FlowLogIds=[flow_log_id])
  print("Deleted flow logs successfully")

#This function reformats strings for the purpose of organizing console printouts. It returns a string of a specified length that consists of
#an input string as well as padding through whitespace. It accepts two arguments:
#string - a string to be reformatted.
#max_length - a max length to impose on the return string.

def string_reformatter(string, max_length):
  for i in range(len(string), max_length):
    string = string + " "
  string = string + "| "
  return string

#This function converts from Unix time (seconds since last Unix epoch) to a year-month-day hour-minute-second format. It returns this reformatted
#time in the form of a string. It accepts a single argument:
#time - the Unix time as a string. 

def convert_from_unix_time(time):
  timestamp = datetime.datetime.fromtimestamp(int(time))
  reformatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
  return reformatted_timestamp

#This function accepts a single argument, "number" (a string) representing a protocol number and returns the corresponding protocol name. 

def return_protocol_name(number):
  if number == "6":
    return "TCP"
  elif number == "17":
    return "UDP"
  elif number == "1":
    return "ICMP"
  else:
    return number

#This function downloads a file from an Amazon S3 bucket and prints to the console all non-encrypted traffic. 
#The file is then read line-by-line, with each line being split by spaces and reformatted to look better in the console. Only lines that 
#represent non-encrypted traffic (non-port-443) are printed.
#It accepts three arguments:
#BUCKET_NAME - the bucket name to download a file from. The file is downloaded as a .log.gz file and unzipped using gzip before reading.
#key - the S3 key of the file to download from the bucket.
#s3 - a low-level boto3 client representing the Amazon Simple Storage Service.


def filter_logs(BUCKET_NAME, key, s3):
  try:
    s3.meta.client.download_file(BUCKET_NAME, key, 'log_01.log.gz') #download a par
    with gzip.open('log_01.log.gz', 'rb') as file:
      file_content = file.read();
      records_list = file_content.splitlines()
      print("Destination Port | Destination IP   | Source Port | Source IP       | Start Time          | Account ID   | Protocol  | Action |")
      for record in range(1, len(records_list)):
        split_record = records_list[record].split()
        source_port = split_record[5].decode('utf-8')
        destination_port = split_record[6].decode('utf-8')
        source_ip = split_record[3].decode('utf-8')
        destination_ip = split_record[4].decode('utf-8')
        start_time = split_record[10].decode('utf-8')
        account_id = split_record[1].decode('utf-8')
        protocol = split_record[7].decode('utf-8')
        protocol_name = return_protocol_name(protocol)
        action = split_record[12].decode('utf-8')
        if source_port != '443' and destination_port != '443':          
          source_port_reformatted = string_reformatter(source_port, 12)
          destination_port_reformatted = string_reformatter(destination_port, 17)
          source_ip_reformatted = string_reformatter(source_ip, 16)
          destination_ip_reformatted = string_reformatter(destination_ip, 17)
          start_time = convert_from_unix_time(start_time)
          protocol_reformatted = string_reformatter(protocol_name, 10)
          action_reformatted = string_reformatter(action, 7) 
          print(destination_port_reformatted + destination_ip_reformatted + source_port_reformatted + source_ip_reformatted + start_time + " | " + account_id + " | " + protocol_reformatted + action_reformatted)
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
      print("The object with the specified key does not exist.")
    else:
      raise

#This function is used to query an Amazon S3 bucket to check if new flow log files have been published. If so, it calls filter_logs()
#to download and parse the new files. The function accepts five arguments:
#BUCKET_NAME - the name of the bucket to query (string).
#PREFIX - a prefix (string) to exclude extraneous files within the bucket by only finding files that begin with "AWSLogs/"
#TOTAL_OBJECTS - the running total of files within the bucket. When all files within the bucket have been tallied up, this number and the tally are compared.
#paginator - a boto3 feature that ensures that the total number of files in the bucket can be gotten, instead of just 1000
#client - a low-level boto3 client representing the Amazon Simple Storage Service. It is not actually used in this function, but is rather just passed
#on to filter_logs()

def get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, client):
  num_objects = 0
  for page in paginator.paginate(Bucket=BUCKET_NAME):
    key_name = 'Contents'
    if key_name in page:
      contents_list = page[key_name]
      for object in contents_list: #iterate through every object in this particular page
        key = object['Key'] #get a particular object's key
        num_objects += 1 #increment the total count of objects
        if num_objects > TOTAL_OBJECTS: #check if the number of objects registered is greater than the previous amount
          filter_logs(BUCKET_NAME, key, client) #download that particular file and check if there is non port 443 traffic.     
  print("Number of objects: " + str(num_objects))
  return num_objects 

#This function returns the user's default region.

def get_default_region():
  ec2_client = boto3.client('ec2')
  region = ec2_client.meta.region_name
  return region

#The main loop of the function. Initializes the EC2 and S3 clients, as well as TOTAL_OBJECTS, BUCKET_NAME, and other variables, creates the paginator
#and the bucket, and then enters a loop where every 60 secodns, get_num_objects() is called to get the total number of objects in the bucket to check
#if new flow log records have been published. The user can break out of the loop by hitting CTRL+C, which will delete the created bucket and Flow Log and
#exit the program. The function accepts two arguments:
#VPC_ID - the ID of the VPC to monitor. Passed in by the start() function.
#REGION - the region in which to create the client. Passed in by the start() function.



def mainloop(VPC_ID, REGION):
  BUCKET_NAME = VPC_ID + "-flow-log-storage" #bucket name to be monitored.
  PREFIX = "AWSLogs/" #prefix (used to filter out extraneous files from the filtering process)
  TOTAL_OBJECTS = 0 #current total number of objects in the bucket (used to check if new objects have been added)

  ec2_client = boto3.client('ec2', region_name = REGION)
  default_region = get_default_region()
  client = boto3.client('s3') #create client
  BUCKET_ARN = "arn:aws:s3:::" + BUCKET_NAME
  paginator = client.get_paginator('list_objects_v2') #create a reusable paginator
  print("Creating bucket " + BUCKET_NAME + "...")
  create_s3_bucket(client, BUCKET_NAME, default_region)
  flow_log_id = create_vpc_flow_log(ec2_client, VPC_ID, BUCKET_ARN)
  s3 = boto3.resource('s3')
  try:
    while True:
      TOTAL_OBJECTS = get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, s3) #update the number of registered objects in the bucket. we pass the bucket name and current number of registered objects to this function.
      print("Querying the bucket for additional objects...")
      time.sleep(60)
  except KeyboardInterrupt:
    print("Shutting down...")
    delete_s3_bucket(BUCKET_NAME, client, s3)
    delete_vpc_flow_log(ec2_client, flow_log_id)
    if os.path.exists("log_01.log.gz"):
      os.remove('log_01.log.gz')
    pass


#This function returns a list of AWS regions.

def get_regions():
  ec2 = boto3.client('ec2')
  response = ec2.describe_regions()
  endpoints = response['Regions']
  regions = []
  for endpoint in endpoints:
    regions.append(endpoint['RegionName'])
  return regions

#This function serves the purpose of receiving and parsing input from the user on which VPC they would like to monitor and which region they would like to
#create the boto3 EC2 client in. 


def start():
  print("Welcome to the AWS VPC Flow Log parser.")
  regions = get_regions()
  input_id = None
  input_region = None
  break_loop = False
  while break_loop == False:
    input_region = input("Please enter the region containing the VPC that you would like to monitor: ")
    valid_region = False
    for region in regions:
      if region == input_region:
        valid_region = True
    ec2 = boto3.client('ec2', region_name = input_region)
    response = ec2.describe_vpcs()
    instance_list = response['Vpcs']
    vpc_ids = []
    index = 0
    for instance in instance_list:
      vpc_id = instance['VpcId']
      print(str(index) + ". " + vpc_id)
      vpc_ids.append(vpc_id)
      index += 1
    input_id = input("Please enter the VPC ID of the VPC that you would like to monitor: ")
    valid_id = False
    for vpc_id in vpc_ids:
      if input_id == vpc_id:
        valid_id = True
    if valid_id == True and valid_region == True:
      break_loop = True
    else:
      print("Invalid ID or region entered. Please try again.")
  mainloop(input_id, input_region)

if __name__ == "__main__":
  start()






