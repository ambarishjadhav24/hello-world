#
# Copyright © 2019 Acoustic, L.P. All rights reserved.
#
# NOTICE: This file contains material that is confidential and proprietary to
# Acoustic, L.P. and/or other developers. No license is granted under any intellectual or
# industrial property rights of Acoustic, L.P. except as may be provided in an agreement with
# Acoustic, L.P. Any unauthorized copying or distribution of content from this file is
# prohibited.
#

FROM wce-wrtp-docker-local.artifactory.swg-devops.com/base-images/precs-job/precs-python-ml:43
#ADD . /app
COPY {{PYTHON_FOLDER}}/* /app/
#COPY parameterized_most_pop/* /app/
#COPY view_to_buy/* /app/
#COPY collaborative_filtering/* /app/
COPY get_yaml_configs.py /app/
COPY save_time_log.py /app/
COPY {{GLOBAL_CONFIG_FILE}} /app/globalconfig.ini
COPY deploy/{{DEPLOY_FILE_LOCATION}}/{{JOB_DEPLOY_NAME}} /app/precs-python-ml.yaml

#RUN mkdir -p /app
#WORKDIR /app
#ADD parameterized_most_pop /app/parameterized_most_pop
#ADD view_to_buy /app/view_to_buy
#ADD collaborative_filtering /app/collaborative_filtering
#COPY requirements.txt /app

#ADD . /app
WORKDIR /app

RUN pip install -r requirements.txt
