import boto3
import botocore
import gzip
import time
import datetime
import os
import random
import string
import json
import logging


logger = logging.getLogger(__name__)

#This function creates an S3 bucket to store flow log records. It accepts three arguments:
#client - a low-level boto3 client representing Amazon Simple Storage Service
#BUCKET_NAME - the name of the bucket to be created. Consists of the ID of the VPC being monitored and "-flow-log-storage"
#REGION - the region in which to create the bucket. Must be the same as the AWS Default Region in the AWS CLI config file.

class VPCFlowLogParser(object):

  def __init__(self, name):
    self.name = name

  def create_s3_bucket(self, client, BUCKET_NAME, REGION):
    location = {'LocationConstraint': REGION}
    try:
      response = client.create_bucket(Bucket=BUCKET_NAME, 
                                      CreateBucketConfiguration=location)
      logger.info("Created bucket successfully")
      return 0
    except botocore.exceptions.ClientError as e:
      logger.error("An error occurred when attempting to create a bucket. "
            "This may be because a bucket of that name already exists, or" 
            "because of a different reason. Shutting down...")
      return 1

  #This function first deletes the contents of an S3 bucket, and then the bucket itself. It is called when the program execution is terminated via
  #a keyboard interrupt. It accepts three arguments:
  #BUCKET_NAME - the name of the bucket to delete.
  #CLIENT - a low-level boto3 client representing Amazon Simple Storage Service
  #RESOURCE - a boto3 resource representing Amazon Simple Storage Service. 

  def delete_s3_bucket(self, BUCKET_NAME, CLIENT, RESOURCE):
    try:
      bucket = RESOURCE.Bucket(BUCKET_NAME)
      bucket.objects.all().delete()
      response = CLIENT.delete_bucket(Bucket=BUCKET_NAME)
      logger.info("Deleted bucket successfully")
    except botocore.exceptions.ClientError as e:
      logger.error("An error occurred when attempting to delete an S3 bucket. "
            "Shutting down...")

  #This function sets up a VPC Flow Log to monitor an AWS EC2 instance. It returns the flow log ID of the flow log created, if successful. If there is an error, 1 is returned.
  #It accepts three arguments:
  #client - a low-level boto3 client representing the Amazon Elastic Compute Cloud.
  #VPC_ID - the ID of the VPC to create Flow Logs for.
  #BUCKET_ARN - the ARN (Amazon Resource Name) of the bucket in which to store Flow Log files. 

  def create_vpc_flow_log(self, client, VPC_ID, BUCKET_ARN):
    try:
      logger.info("Creating flow logs...")
      response = client.create_flow_logs(ResourceIds=[VPC_ID], ResourceType='VPC', 
                                         TrafficType='ALL', LogDestinationType='s3',
                                         LogDestination=BUCKET_ARN, 
                                         MaxAggregationInterval=60);
      logger.info("Created flow logs successfully")
      flow_log_id = response['FlowLogIds'][0]
      logger.info("Flow log ID: " + flow_log_id)
      return flow_log_id
    except botocore.exceptions.ClientError as e:
      logger.error("An error occurred when attempting to create VPC flow logs. "
            "Shutting down...")
      return 1

  #This function deletes a VPC Flow Log. It is called when the program execution is terminated via a keyboard interrupt.
  #It accepts two arguments:
  #client - a low-level boto3 client representing the Amazon Elastic Compute Cloud.
  #flow_log_id - the ID of the flow log to delete.

  def delete_vpc_flow_log(self, client, flow_log_id):
    try:
      logger.info("Deleting flow logs...")
      response = client.delete_flow_logs(FlowLogIds=[flow_log_id])
      logger.info("Deleted flow logs successfully")
      return 0
    except botocore.exceptions.ClientError as e:
      logger.error("An error occurred when attempting to delete VPC flow logs.")
      return 1
  #This function reformats strings for the purpose of organizing console printouts. It returns a string of a specified length that consists of
  #an input string as well as padding through whitespace. It accepts two arguments:
  #string - a string to be reformatted.
  #max_length - a max length to impose on the return string.

  def string_reformatter(self, string, max_length):
    for i in range(len(string), max_length):
      string = string + " "
    string = string + "| "
    return string

  #This function converts from Unix time (seconds since last Unix epoch) to a year-month-day hour-minute-second format. It returns this reformatted
  #time in the form of a string. It accepts a single argument:
  #time - the Unix time as a string. 

  def convert_from_unix_time(self, time):
    timestamp = datetime.datetime.fromtimestamp(int(time))
    reformatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return reformatted_timestamp

  #This function accepts a single argument, "number" (a string) representing a protocol number and returns the corresponding protocol name. 

  def return_protocol_name(self, number):
    protocol_dict = {
      "6": "TCP",
      "17": "UDP",
      "1": "ICMP"
    }
    return protocol_dict.get(number, number)

  def dump_to_json(self, records, accepted_traffic_tally, rejected_traffic_tally, tcp_traffic_tally, udp_traffic_tally, other_traffic_tally):
    now = datetime.datetime.now()
    date_suffix = now.strftime("%m-%d-%Y-%H-%M-%S")
    JSON_FILENAME = "log-" + date_suffix + ".json"
    JSON_FILE_OBJ = open(JSON_FILENAME, "a+")
    return_dict = {
      "Accepted Traffic Tally": accepted_traffic_tally,
      "Rejected Traffic Tally": rejected_traffic_tally,
      "TCP Traffic Tally": tcp_traffic_tally,
      "UDP Traffic Tally": udp_traffic_tally,
      "Other Traffic Tally": other_traffic_tally,
      "All traffic": records
    }
    json.dump(return_dict, JSON_FILE_OBJ)
    JSON_FILE_OBJ.close()
   

  #This function downloads a file from an Amazon S3 bucket and prints to the console all non-encrypted traffic. 
  #The file is then read line-by-line, with each line being split by spaces and reformatted to look better in the console. Only lines that 
  #represent non-encrypted traffic (non-port-443) are printed.
  #It accepts three arguments:
  #BUCKET_NAME - the bucket name to download a file from. The file is downloaded as a .log.gz file and unzipped using gzip before reading.
  #key - the S3 key of the file to download from the bucket.
  #s3 - a low-level boto3 client representing the Amazon Simple Storage Service.

  def filter_logs(self, BUCKET_NAME, key, s3, json_output):
    try:
      return_dict = {
        "accepted_traffic_tally": 0,
        "rejected_traffic_tally": 0,
        "tcp_traffic_tally": 0,
        "udp_traffic_tally": 0,
        "other_traffic_tally": 0
      }
      s3.meta.client.download_file(BUCKET_NAME, key, 'log_01.log.gz') 
      with gzip.open('log_01.log.gz', 'rb') as file:
        file_content = file.read();
        records_list = file_content.splitlines()
        print("Destination Port | Destination IP   | Source Port | "
              "Source IP       | Start Time          | Account ID   | "
              "Protocol  | Action |")
        for record in range(1, len(records_list)):
          split_record = records_list[record].split()
          source_port = split_record[5].decode('utf-8')
          destination_port = split_record[6].decode('utf-8')
          if source_port != '443' and destination_port != '443':
            source_port_reformatted = self.string_reformatter(source_port, 12)
            destination_port_reformatted = self.string_reformatter(destination_port, 17)
            source_ip_reformatted = self.string_reformatter(split_record[3].decode('utf-8'), 16)
            destination_ip_reformatted = self.string_reformatter(split_record[4].decode('utf-8'), 17)
            start_time = self.convert_from_unix_time(split_record[10].decode('utf-8'))
            protocol = split_record[7].decode('utf-8')
            account_id = split_record[1].decode('utf-8')
            protocol_reformatted = self.string_reformatter(self.return_protocol_name(protocol), 10)
            action_reformatted = self.string_reformatter(split_record[12].decode('utf-8'), 7)


            json_source_ip = split_record[3].decode('utf-8')
            json_destination_ip = split_record[4].decode('utf-8')
            json_start_time = split_record[10].decode('utf-8')
            json_protocol = split_record[7].decode('utf-8')
            json_account_id = split_record[1].decode('utf-8')
            json_action = split_record[12].decode('utf-8')

            json_dict = {
              "Source IP": json_source_ip,
              "Destination IP": json_destination_ip,
              "Source Port": source_port,
              "Destination Port": destination_port,
              "Start time": json_start_time,
              "Protocol": json_protocol,
              "Account ID": json_account_id,
              "Action": json_action
            }

            json_output.append(json_dict)

            print(destination_port_reformatted + destination_ip_reformatted + 
                  source_port_reformatted + source_ip_reformatted + start_time + 
                  " | " + account_id + " | " + protocol_reformatted + 
                  action_reformatted)
            if split_record[12].decode('utf-8') == "ACCEPT":
              return_dict['accepted_traffic_tally'] += 1
            else:
              return_dict['rejected_traffic_tally'] += 1
            if self.return_protocol_name(protocol) == "TCP":
              return_dict['tcp_traffic_tally'] += 1
            elif self.return_protocol_name(protocol) == "UDP":
              return_dict['udp_traffic_tally'] += 1
            else:
              return_dict['other_traffic_tally'] += 1
      return return_dict 
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        logger.error("The object with the specified key does not exist.")
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

  def get_num_objects(self, BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, client):
    num_objects = 0
    new_keys = []
    for page in paginator.paginate(Bucket=BUCKET_NAME):
      key_name = 'Contents'
      if key_name in page:
        contents_list = page[key_name]
        for object in contents_list: #iterate through every object in this particular page
          key = object['Key'] #get a particular object's key
          num_objects += 1 #increment the total count of objects
          if num_objects > TOTAL_OBJECTS: #check if the number of objects registered is greater than the previous amount
            new_keys.append(key)
            #filter_logs(BUCKET_NAME, key, client) #download that particular file and check if there is non port 443 traffic.     
    logger.info("Number of objects: " + str(num_objects))
    #return num_objects 
    return new_keys

  #This function returns the user's default region.

  def get_default_region(self):
    ec2_client = boto3.client('ec2')
    region = ec2_client.meta.region_name
    return region


  def cleanup(self, BUCKET_NAME, s3_client, s3_resource, ec2_client, flow_log_id):
    self.delete_s3_bucket(BUCKET_NAME, s3_client, s3_resource)
    self.delete_vpc_flow_log(ec2_client, flow_log_id)
    if os.path.exists("log_01.log.gz"):
      os.remove('log_01.log.gz')

  #This function dry runs the create_flow_logs function to make sure that the user has sufficient permissions to
  #create flow logs. If so, 0 is returned. If not, 1 is returned.

  def check_logging_permissions(self, client, BUCKET_ARN, VPC_ID):
    try:
      response = client.create_flow_logs(DryRun=True, ResourceIds=[VPC_ID], ResourceType='VPC', 
                                         TrafficType='ALL', LogDestinationType='s3',
                                         LogDestination=BUCKET_ARN, 
                                         MaxAggregationInterval=60);
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "DryRunOperation":
        logger.info("You have sufficient permissions to create flow logs.")
        return 0
      else:
        logger.error("You don't have sufficient permissions to create flow logs. Quitting...")
        return 1


  #The main loop of the function. Initializes the EC2 and S3 clients, as well as TOTAL_OBJECTS, BUCKET_NAME, and other variables, creates the paginator
  #and the bucket, and then enters a loop where every 60 secodns, get_num_objects() is called to get the total number of objects in the bucket to check
  #if new flow log records have been published. The user can break out of the loop by hitting CTRL+C, which will delete the created bucket and Flow Log and
  #exit the program. The function accepts two arguments:
  #VPC_ID - the ID of the VPC to monitor. Passed in by the start() function.
  #REGION - the region in which to create the client. Passed in by the start() function.

  def mainloop(self, VPC_ID, REGION):
    accepted_traffic_tally = 0
    rejected_traffic_tally = 0
    tcp_traffic_tally = 0
    udp_traffic_tally = 0
    other_traffic_tally = 0
    json_output = []
    rand_string = "".join(random.choices(string.ascii_lowercase + string.digits, k=8)) #generate random string to distinguish bucket for extra insurance
    BUCKET_NAME = VPC_ID + "-fls-" + rand_string #bucket name to be monitored.
    PREFIX = "AWSLogs/" #prefix (used to filter out extraneous files from the filtering process)
    TOTAL_OBJECTS = 0 #current total number of objects in the bucket (used to check if new objects have been added)
    ec2_client = boto3.client('ec2', region_name = REGION)
    default_region = self.get_default_region()
    client = boto3.client('s3') #create client
    BUCKET_ARN = "arn:aws:s3:::" + BUCKET_NAME
    check_permissions_result = self.check_logging_permissions(ec2_client, BUCKET_ARN, VPC_ID)
    if check_permissions_result == 1:
      return
    paginator = client.get_paginator('list_objects_v2') #create a reusable paginator
    logger.info("Creating bucket " + BUCKET_NAME + "...")
    create_bucket_result = self.create_s3_bucket(client, BUCKET_NAME, default_region)
    if create_bucket_result == 1:
      return
    flow_log_id = self.create_vpc_flow_log(ec2_client, VPC_ID, BUCKET_ARN)
    if flow_log_id == 1:
      return
    s3 = boto3.resource('s3')
    try:
      while True:
        print("Querying the bucket for objects...")
        #TOTAL_OBJECTS = get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, 
        #                                paginator, s3) #update the number of registered objects in the bucket. we pass the bucket name and current number of registered objects to this function.
        new_keys = self.get_num_objects(BUCKET_NAME, PREFIX, TOTAL_OBJECTS, paginator, s3)
        if len(new_keys) != 0:
          TOTAL_OBJECTS += len(new_keys)
          for key in new_keys:
            returned_dict = self.filter_logs(BUCKET_NAME, key, s3, json_output)
            accepted_traffic_tally += returned_dict['accepted_traffic_tally']
            rejected_traffic_tally += returned_dict['rejected_traffic_tally']
            tcp_traffic_tally += returned_dict['tcp_traffic_tally']
            udp_traffic_tally += returned_dict['udp_traffic_tally']
            other_traffic_tally += returned_dict['other_traffic_tally']
            
          logger.info("Querying the bucket for additional objects...")
        time.sleep(60)
    except KeyboardInterrupt:
      logger.info("Shutting down...")
      #delete_s3_bucket(BUCKET_NAME, client, s3)
      #delete_vpc_flow_log(ec2_client, flow_log_id)
      #if os.path.exists("log_01.log.gz"):
      #  os.remove('log_01.log.gz')
      self.cleanup(BUCKET_NAME, client, s3, ec2_client, flow_log_id)
      print("LOG SUMMARY ===============================")
      print("Accepted traffic: %d" % accepted_traffic_tally)
      print("Rejected traffic: %d" % rejected_traffic_tally)
      print("TCP traffic: %d" % tcp_traffic_tally)
      print("UDP traffic: %d" % udp_traffic_tally)
      print("Other traffic: %d" % other_traffic_tally)
      print("===========================================")
      self.dump_to_json(json_output, 
                   accepted_traffic_tally, 
                   rejected_traffic_tally, 
                   tcp_traffic_tally, 
                   udp_traffic_tally, 
                   other_traffic_tally);
      pass

  #This function returns a list of AWS regions.

  def get_regions(self):
    ec2 = boto3.client('ec2')
    response = ec2.describe_regions()
    endpoints = response['Regions']
    regions = []
    for endpoint in endpoints:
      regions.append(endpoint['RegionName'])
    return regions

  #This function serves the purpose of receiving and parsing input from the user on which VPC they would like to monitor and which region they would like to
  #create the boto3 EC2 client in. 

  def action(self):
    print("Welcome to the AWS VPC Flow Log parser.")
    regions = self.get_regions()
    input_id = None
    input_region = None
    break_loop = False
    while not break_loop:
      input_region = input("Please enter the region containing the VPC "
                           "that you would like to monitor: ")
      valid_region = False
      if input_region in regions:
        valid_region = True
      ec2 = boto3.client('ec2', region_name = input_region)
      filter_for_running_vpcs = [{}]
      response = ec2.describe_vpcs()
      instance_list = response['Vpcs']
      print("VPCs in that region: ")
      vpc_ids = [instance['VpcId'] for instance in instance_list]
      
      print(*vpc_ids, sep="\n")
      
      input_id = input("Please enter the VPC ID of the VPC that you would"
                       " like to monitor: ")
      valid_id = False
      if input_id in vpc_ids:
        valid_id = True
      if valid_id and valid_region:
        break_loop = True
      else:
        logger.error("Invalid ID or region entered. Please try again.")
    self.mainloop(input_id, input_region)

#if __name__ == "__main__":
#  start()

classInstance1 = VPCFlowLogParser("instance1")
classInstance1.action()

#ec2_client = boto3.client('ec2')
#try:
#  response = ec2_client.create_flow_logs(DryRun=True, ResourceIds=["vpc-080c763ae07c79afb"], ResourceType='VPC', 
#                                         TrafficType='ALL', LogDestinationType='s3',
#                                         LogDestination="arn:aws:s3:::benhuangbucket2", 
#                                         MaxAggregationInterval=60);
#except botocore.exceptions.ClientError as e:
#  print("There was an error")
#  if e.response['Error']['Code'] == "DryRunOperation":
#    print("Just a dry run operation, nothing to see here....")

#  else:
#    print("You don't have sufficient permissions to run this API. Quitting...")



#delete_vpc_flow_log(ec2_client, "fl-0dcfcf7114327abe7")


#List_of_Policies =  client.list_user_policies(UserName=key['UserName'])
#for key in List_of_Policies['PolicyNames']:
#  print key['PolicyName']