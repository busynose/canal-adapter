FROM makeblock/alpine

ARG tmp_api_version=default_value
ENV API_VERSION=$tmp_api_version

ADD canalAdapter /app/
ADD config.yml /app/

WORKDIR /app

EXPOSE 8080

RUN ls -l
CMD ["./canalAdapter"]