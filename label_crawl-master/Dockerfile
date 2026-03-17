FROM swr.cn-east-3.myhuaweicloud.com/beosin-boom/python3.11-slim-bullseye:V1.0
ADD ./requirements.txt /label-crawl/requirements.txt
# 修改docker container系统的时间
ENV TIMEZONE Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIMEZONE /etc/localtime && \
  pip install -i http://123.60.136.30:38080/simple --trusted-host 123.60.136.30:38080 -r /label-crawl/requirements.txt

ADD ./src /label-crawl/src
WORKDIR /label-crawl/src

ENV PROJECT_NAME='label-crawl'

CMD ["python",  "/label-crawl/src/main.py"]