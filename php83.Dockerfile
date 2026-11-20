FROM composer:2.10@sha256:2cf275428757e5435a667bfc0d038b14520ca02be2b1b70d44a05c8c19e2de48 AS composer

ARG TESTING=false
ENV TESTING=$TESTING

WORKDIR /usr/local/src/

COPY composer.lock /usr/local/src/
COPY composer.json /usr/local/src/

RUN composer update --ignore-platform-reqs --optimize-autoloader \
    --no-plugins --no-scripts --prefer-dist
    
FROM appwrite/utopia-base:php-8.3-0.2.1@sha256:e6519df8822d8dc6e4abcdaf7e3eca8ce2199f23bddc932b2a1835722968a542 as compile

ENV PHP_MONGO_VERSION=2.2.1
    
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN \
  apk update \
  && apk add --no-cache postgresql-libs postgresql-dev make automake autoconf gcc g++ git \
  && docker-php-ext-install opcache pgsql pdo_mysql pdo_pgsql \
  && rm -rf /var/cache/apk/*

## MongoDB Extension
FROM compile AS mongodb
RUN \
  git clone --depth 1 --branch $PHP_MONGO_VERSION https://github.com/mongodb/mongo-php-driver.git \
  && cd mongo-php-driver \
  && git submodule update --init \
  && phpize \
  && ./configure \
  && make && make install

FROM compile AS final

LABEL maintainer="team@appwrite.io"

WORKDIR /usr/src/code

RUN echo extension=mongodb.so >> /usr/local/etc/php/conf.d/mongodb.ini

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

RUN echo "opcache.enable_cli=1" >> $PHP_INI_DIR/php.ini

RUN echo "memory_limit=1024M" >> $PHP_INI_DIR/php.ini

COPY --from=composer /usr/local/src/vendor /usr/src/code/vendor
COPY --from=mongodb /usr/local/lib/php/extensions/no-debug-non-zts-20230831/mongodb.so /usr/local/lib/php/extensions/no-debug-non-zts-20230831/

# Add Source Code
COPY . /usr/src/code

CMD [ "tail", "-f", "/dev/null" ]

