#!/bin/bash
set -e

# start ssm
systemctl start amazon-ssm-agent

# Update system and install dependencies
yum update -y
yum install -y docker awslogs jq amazon-cloudwatch-agent curl

# Start and enable services
systemctl start docker
systemctl enable docker
systemctl start awslogsd
systemctl enable awslogsd

# Configure CloudWatch Logs
cat > /etc/awslogs/awslogs.conf << 'EOF'
[general]
state_file = /var/lib/awslogs/agent-state

[/var/log/docker]
file = /var/log/docker
log_group_name = ${log_group_name}
log_stream_name = {instance_id}/docker
datetime_format = %Y-%m-%d %H:%M:%S

[/opt/hydro-api/logs/hf.log]
file = /opt/hydro-api/logs/hf.log
log_group_name = ${log_group_name}
log_stream_name = {instance_id}/hf.log
datetime_format = %Y-%m-%d %H:%M:%S
EOF

# Configure region for awslogs
sed -i "s/region = us-east-1/region = ${aws_region}/" /etc/awslogs/awscli.conf
systemctl restart awslogsd

# Configure CloudWatch Agent for memory metrics
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOF'
{
  "metrics": {
    "namespace": "System/Linux",
    "metrics_collected": {
      "mem": {
        "measurement": [
          {"name": "mem_used_percent", "rename": "MemoryUtilization"}
        ],
        "metrics_collection_interval": 60
      }
    }
  }
}
EOF

# Start CloudWatch Agent
systemctl start amazon-cloudwatch-agent
systemctl enable amazon-cloudwatch-agent

# Create helper function to fetch secrets with retry
fetch_secret_with_retry() {
    local secret_arn=$1
    local max_attempts=12
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Attempting to fetch secret (attempt $attempt of $max_attempts)..." >&2
        
        local secret_value
        secret_value=$(aws secretsmanager get-secret-value \
            --secret-id "$secret_arn" \
            --region ${aws_region} \
            --query SecretString \
            --output text 2>/dev/null) || true
            
        if [ -n "$secret_value" ] && [ "$(echo "$secret_value" | wc -l)" -eq 1 ]; then
            printf '%s' "$secret_value"
            return 0
        fi

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Attempt $attempt failed." >&2
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 10 seconds before retry..." >&2
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to fetch secret after $max_attempts attempts" >&2
    return 1
}

echo "Fetching database credentials..."
DB_SECRETS=$(fetch_secret_with_retry "${secrets_manager_arn}")
if [ $? -ne 0 ]; then
    echo "Failed to fetch database credentials"
    exit 1
fi

# Use printf to safely handle special characters
DB_USER=$(printf '%s' "$DB_SECRETS" | jq -r '.username')
DB_PASSWORD=$(printf '%s' "$DB_SECRETS" | jq -r '.password')

echo "Fetching registry credentials..."
REGISTRY_SECRETS=$(fetch_secret_with_retry "${registry_secret_arn}")
if [ $? -ne 0 ]; then
    echo "Failed to fetch registry credentials"
    exit 1
fi

REGISTRY_USER=$(printf '%s' "$REGISTRY_SECRETS" | jq -r '.username')
REGISTRY_TOKEN=$(printf '%s' "$REGISTRY_SECRETS" | jq -r '.token')

echo "Logging into registry..."
printf '%s' "$REGISTRY_TOKEN" | docker login ${registry_url} -u "$REGISTRY_USER" --password-stdin

echo "Creating application directory..."
mkdir -p /opt/hydro-api

# Download function with retry logic because this failed a couple times while testing
download_hydrofabric_data() {
    local max_attempts=5
    local attempt=1
    local wait_time=30
    local target_dir="/tmp/hydro_api"
    
    # Ensure target directory exists
    mkdir -p "$target_dir"
    
    while [ $attempt -le $max_attempts ]; do
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading Hydrofabric data (attempt $attempt of $max_attempts)..."
        
        # Clean up any partial downloads
        rm -f "$target_dir/hydrofabric_data.tgz"
        
        # Clear system cache to free up memory
        echo 3 > /proc/sys/vm/drop_caches
        
        # Download with resume capability and basic error handling
        if aws s3 cp --no-progress \
            "s3://${hydro_s3_bucket}/hydrofabric_data.tgz" \
            "$target_dir/" \
            --region ${aws_region} \
            --cli-connect-timeout 30 \
            --cli-read-timeout 60; then
            
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Download completed successfully"
            
            # Verify file integrity
            if [ -f "$target_dir/hydrofabric_data.tgz" ] && [ -s "$target_dir/hydrofabric_data.tgz" ]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] File verification passed"
                return 0
            else
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloaded file is empty or missing"
            fi
        fi
        
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Attempt $attempt failed"
        
        if [ $attempt -lt $max_attempts ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting $wait_time seconds before retry..."
            sleep $wait_time
            # Increase wait time for next attempt
            wait_time=$((wait_time * 2))
        fi
        
        attempt=$((attempt + 1))
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to download after $max_attempts attempts"
    return 1
}

# Extract function with verification because this failed a couple times while testing
extract_hydrofabric_data() {
    local source_file="/tmp/hydro_api/hydrofabric_data.tgz"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extracting Hydrofabric data..."
    
    # Clear system cache again before extraction
    echo 3 > /proc/sys/vm/drop_caches
    
    # Create target directory if it doesn't exist
    mkdir -p /Hydrofabric
    
    # Extract with error checking
    if tar -xzf "$source_file" -C / ; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extraction completed successfully"
        
        # Verify extraction
        if [ -d "/Hydrofabric" ] && [ "$(ls -A /Hydrofabric)" ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extraction verification passed"
            # Clean up the downloaded archive to free space
            rm -f "$source_file"
            return 0
        else
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extracted directory is empty or missing"
            return 1
        fi
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extraction failed"
        return 1
    fi
}

# Main execution flow for Hydrofabric data
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Hydrofabric data setup..."

# Attempt download with retry
if ! download_hydrofabric_data; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to download Hydrofabric data"
    exit 1
fi

# Attempt extraction
if ! extract_hydrofabric_data; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to extract Hydrofabric data"
    exit 1
fi

# Set appropriate permissions as this will be volume mounted to the Docker container
chmod -R 755 /Hydrofabric
chown -R root:root /Hydrofabric

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Hydrofabric data setup completed successfully"


echo "Creating docker-compose configuration..."
# Use explicit environment file to handle special characters
# Get the Local IP and add it to DJANGO_ALLOWED_HOSTS for target group / ELB health checks
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
LOCAL_IP=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

cat > /opt/hydro-api/.env << EOF
DB_HOST=${db_host}
DB_PORT=${db_port}
DB_NAME=${db_name}
LOG_DIR=/opt/hydro-api/logs/
DEPLOYMENT_TIMESTAMP=${deployment_timestamp}
DB_USER=$(printf '%s' "$DB_USER" | sed 's/[\/&]/\\&/g')
DB_PASSWORD=$(printf '%s' "$DB_PASSWORD" | sed 's/[\/&]/\\&/g')
DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1,.nextgenwaterprediction.com,$LOCAL_IP
AWS_REGION=${aws_region}
S3_BUCKET=${hydro_s3_bucket}
SECRETS_ARN=${secrets_manager_arn}
ENVIRONMENT=${environment}
EOF

cat > /opt/hydro-api/docker-compose.yml << 'EOF'
version: '3.8'
services:
  api:
    image: ${docker_image}
    network_mode: "host"
    env_file: .env
    ports:
      - "${container_port}:8000"
    volumes:
      - /Hydrofabric:/Hydrofabric:rw
      - /opt/hydro-api/logs/:/opt/hydro-api/logs/:rw
    restart: always
    healthcheck:
      test: ["CMD", "wget", "--spider", "--quiet", "http://localhost:8000/version/"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
EOF

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting application..."
cd /opt/hydro-api

# Install docker-compose if not present
if ! command -v docker-compose &> /dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Installing docker-compose..."
    curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

# Create systemd service for Docker Compose
cat > /etc/systemd/system/hydro-api.service << EOF
[Unit]
Description=Hydro API Docker Compose Service
After=docker.service
Requires=docker.service

[Service]
Restart=always
WorkingDirectory=/opt/hydro-api
ExecStart=/usr/local/bin/docker-compose up
ExecStop=/usr/local/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable the service
systemctl daemon-reload
systemctl enable hydro-api.service
systemctl start hydro-api.service

echo "Setting up log rotation..."
cat > /etc/logrotate.d/docker << 'EOF'
/var/log/docker {
    rotate 5
    daily
    compress
    size 50M
    missingok
    delaycompress
    copytruncate
}

/opt/hydro-api/logs/hf.log {
    rotate 5
    daily
    compress
    size 50M
    missingok
    delaycompress
    copytruncate
}
EOF

echo "User data script completed successfully"
