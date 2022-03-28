# Create MELT Docker Manually

Once the license is acquired from MELT Team, please follow the below process to setup MELT.
- Go to https://melt.igs.umaryland.edu/downloads.php
- Read the agreement and if agreed upon, fill in your details in fields.
- Hit "download MELT"
- A tar.gz file will be downloaded.
- Upload this file at the S3 Results Bucket you specified during the Infra Setup from GWOA Repo.
- Go to the Cromwell EC2 Instance at : /home/ec2-user/gatk-sv/dockerfiles/melt
- Download the file from S3 at this location.
- IMPORTANT : Update MELT Release in Dockerfile (check the tar.gz name and references carefully) and also rename the run_MELT*.sh to the corresponding version.
- Build and deploy the image using below steps:
    ```bash
    ECR_REPO_NAME="sv-pipeline" # The repo name you want to use for this image.
    AWS_REGION=`curl -s http://169.254.169.254/latest/dynamic/instance-identity/document| grep region | cut -d '"' -f4`
    export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --output text --query 'Account')
    aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    docker build --no-cache --rm -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:melt_docker .
    docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:melt_docker
    ```
