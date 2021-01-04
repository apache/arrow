FROM docker/compose

RUN apk add git python3 py3-pip

COPY . /archery
RUN pip3 install -e archery[docker]

WORKDIR /arrow
ENTRYPOINT ["archery"]
