This project is designed to:
1. Automatically create an AWS VPC flow log for an existing AWS VPC and an S3 bucket to hold flow log records
2. Periodically download flow log records from the bucket, parse them, and display all non 443 (unencrypted) traffic going through the VPC.

Please note that you will have to manually change the variable "VPC_ID" in the function "mainloop()" (line 104) to the VPC ID of the VPC
that you would like to monitor.

