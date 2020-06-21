import boto3
import gzip
import time
import datetime

def create_s3_bucket(client, BUCKET_NAME, REGION):
  location = {'LocationConstraint': REGION}
  response = client.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration=location)
  #print(response)
  print("Created bucket successfully")

def delete_s3_bucket(BUCKET_NAME, CLIENT, RESOURCE):
  bucket = RESOURCE.Bucket(BUCKET_NAME)
  bucket.objects.all().delete()
  response = CLIENT.delete_bucket(Bucket=BUCKET_NAME)
  #print(response)
  print("Deleted bucket successfully")


def create_vpc_flow_log(client, VPC_ID, BUCKET_ARN):
  print("Creating flow logs...")
  response = client.create_flow_logs(ResourceIds=[VPC_ID], ResourceType='VPC', TrafficType='ALL', LogDestinationType='s3', LogDestination=BUCKET_ARN, MaxAggregationInterval=60);
  #print(response);
  print("Created flow logs successfully")
  flow_log_id = response['FlowLogIds'][0]
  print("Flow log ID: " + flow_log_id)
  return flow_log_id

def delete_vpc_flow_log(client, flow_log_id):
  print("Deleting flow logs...")
  response = client.delete_flow_logs(FlowLogIds=[flow_log_id])
  #print(response)
  print("Deleted flow logs successfully")
   
def string_reformatter(string, max_length):
  for i in range(len(string), max_length):
    string = string + " "
  string = string + "| "
  return string

def convert_from_unix_time(time):
  timestamp = datetime.datetime.fromtimestamp(int(time))
  reformatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
  return reformatted_timestamp

def filter_logs(BUCKET_NAME, key, s3):
  try:

    s3.meta.client.download_file(BUCKET_NAME, key, 'log_01.log.gz') #download a par
    with gzip.open('log_01.log.gz', 'rb') as file:
      file_content = file.read();
      #print(file_content)
      records_list = file_content.splitlines()
      #for record in records_list:
      print("Destination Port | Destination IP   | Source Port | Source IP       | Start Time          | Account ID   | Protocol  |")
      for record in range(1, len(records_list)):
        #print(record)
        split_record = records_list[record].split()
        source_port = split_record[5].decode('utf-8')
        destination_port = split_record[6].decode('utf-8')
        source_ip = split_record[3].decode('utf-8')
        destination_ip = split_record[4].decode('utf-8')
        start_time = split_record[10].decode('utf-8')
        account_id = split_record[1].decode('utf-8')
        protocol = split_record[7].decode('utf-8')
        if source_port != '443' and destination_port != '443':          
          source_port_reformatted = string_reformatter(source_port, 12)
          destination_port_reformatted = string_reformatter(destination_port, 17)
          source_ip_reformatted = string_reformatter(source_ip, 16)
          destination_ip_reformatted = string_reformatter(destination_ip, 17)
          start_time = convert_from_unix_time(start_time)
          protocol_reformatted = string_reformatter(protocol, 10)
           
          print(destination_port_reformatted + destination_ip_reformatted + source_port_reformatted + source_ip_reformatted + start_time + " | " + account_id + " | " + protocol_reformatted)
        #print(split_record);

  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
      print("The object with the specified key does not exist.")
    else:
      raise

def get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, client):
  num_objects = 0
  
  for page in paginator.paginate(Bucket=BUCKET_NAME):
      #print(page["Contents"])
    key_name = 'Contents'
    if key_name in page:
      contents_list = page[key_name]
      for object in contents_list: #iterate through every object in this particular page
          
        key = object['Key'] #get a particular object's key
        num_objects += 1 #increment the total count of objects
        if num_objects > TOTAL_OBJECTS: #check if the number of objects registered is greater than the previous amount
          #print("Additional object detected.")
          filter_logs(BUCKET_NAME, key, client) #download that particular file and check if there is non port 443 traffic.
          
          
  print("Number of objects: " + str(num_objects))

  return num_objects 

def mainloop(VPC_ID, REGION):
  BUCKET_NAME = "flowlogstorage01" #bucket name to be monitored.
  PREFIX = "AWSLogs/" #prefix (used to filter out extraneous files from the filtering process)
  TOTAL_OBJECTS = 0 #current total number of objects in the bucket (used to check if new objects have been added)
  ec2_client = boto3.client('ec2')
  client = boto3.client('s3') #create client
  BUCKET_ARN = "arn:aws:s3:::flowlogstorage01"
  paginator = client.get_paginator('list_objects_v2') #create a reusable paginator
  
  create_s3_bucket(client, BUCKET_NAME, REGION)
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
    pass

def start():
  print("Welcome to the AWS VPC Flow Log parser.")
  ec2 = boto3.client('ec2')
  region = ec2.meta.region_name
  response = ec2.describe_vpcs()
  
  instance_list = response['Vpcs']

  vpc_ids = []
  for instance in instance_list:
    vpc_id = instance['VpcId']
    vpc_ids.append(vpc_id)
  print(vpc_ids)
  break_loop = False
  input_id = None
  while break_loop == False:
    input_id = input("Please enter the VPC ID of the VPC that you would like to monitor: ")
    valid_id = False
    for vpc_id in vpc_ids:
      if input_id == vpc_id:
        valid_id = True
    if valid_id == True:
      break_loop = True
    else:
      print("Invalid ID entered. Please try again.")
  mainloop(input_id, region)
    
  


start()

#mainloop()


#s3_client = boto3.client('s3')
#Bucket_name = "flowlogstorage01"
#region = "us-west-1"
#create_s3_bucket(s3_client, Bucket_name, region);
#ec2_client = boto3.client('ec2');
#flow_log_id = "fl-069cb1c1ccac5fdbe";
#delete_vpc_flow_log(ec2_client, flow_log_id)

#BUCKET_NAME = "flowlogstorage01"
#REGION = "us-west-1"
#PREFIX = "AWSLogs/"
#TOTAL_OBJECTS = 0
#client = boto3.client('s3')
#s3 = boto3.resource('s3')
#create_s3_bucket(client, BUCKET_NAME, REGION)
#paginator = client.get_paginator('list_objects_v2')
#get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, s3)


