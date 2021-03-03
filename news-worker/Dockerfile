#syntax=harbor.aigauss.com/docker/dockerfile:experimental
FROM harbor.aigauss.com/base-images/maven:3-jdk-8-slim as builder
# 配置默认工作空间
WORKDIR /app
# copy pom.xml 文件
COPY ./pom.xml .
RUN mvn dependency:purge-local-repository -DgroupId==com.tigerye && \
    mvn -T 4C dependency:go-offline
# 复制所有文件到默认项目空间
# build 项目
COPY . .
RUN mvn dependency:purge-local-repository -DgroupId==com.tigerye && \
    mvn -U -T 4C -DskipTests clean package
RUN mv target/*.jar target/result.jar

FROM harbor.aigauss.com/base-images/openjdk:8 as app
ARG APP_ID=news-worker
ARG APOLLO_META=http://apollo-config-test.aigauss.com:8080
ENV APP_ID=$APP_ID \
    APOLLO_META=$APOLLO_META
#copy builder （上一个阶段的容器） 的 /app/target/result.jar  目录到当前镜像
COPY --from=builder /app/target/result.jar $JAR_PATH

EXPOSE 8080