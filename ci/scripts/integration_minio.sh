#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

source_dir=${1}
build_dir=${2}

mkdir ./minio_integration

# Not ideal but need to run minio server in the background
# and ensure it is up and running.
minio server ./minio_integration --address 127.0.0.1:9000 &
sleep 30

echo '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
          "s3:ListAllMyBuckets",
	  "s3:PutObject",
	  "s3:GetObject",
	  "s3:ListBucket",
	  "s3:PutObjectTagging",
	  "s3:DeleteObject",
	  "s3:GetObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}' > ./limited_policy.json

# Create a user with limited permissions
mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin
mc admin policy add myminio/ no-create-buckets ./limited_policy.json
mc admin user add myminio/ limited limited123
mc admin policy set myminio no-create-buckets user=limited
# Create a bucket
mc mb myminio/existing-bucket

# Run Arrow tests relying on limited permissions user

python -mpytest ${source_dir}/ci/scripts/integration_minio.py
