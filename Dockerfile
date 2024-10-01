# Starting from Debian NodeJS with Python and Postgres container in docker repo
FROM node:lts

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /usr/src/app

# Disabling SSL verification
RUN git config --global http.sslVerify false

# Installing utilities and needed libraries
RUN apt-get -qq update &&\
    apt-get upgrade -y
RUN apt-get -y install libcurl4 \
    curl \
    wget \
    nano \
    bash \
    postgresql-client \
    python3 \
    python3-dev \
    python3-pip \
    gcc \
    g++ \
    make \
    git \
    libpq-dev \
    musl-dev &&\
    apt-get -qq clean
#Installing text editors vim and nano
RUN apt-get update -y --allow-releaseinfo-change && \
    apt-get install -qq nano && \
    apt-get install -qqy vim && \
    # Clean and remove unused packages
    apt-get -y clean && apt-get -y autoclean && apt-get -y autoremove && \
    # Clear caches/tmp and apt lists
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Installing rust 
RUN curl --proto '=https' --tlsv1.2 -o rust.sh -sSf https://sh.rustup.rs 
RUN chmod +x rust.sh;./rust.sh -y
ENV PATH="${PATH}:/root/.cargo/bin"
RUN source "$HOME/.cargo/env"

# Installing python dependencies
RUN pip install -U pip
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install torch==2.1.0 torchvision==0.16.0 torchaudio==2.1.0 --extra-index-url https://download.pytorch.org/whl/cpu
RUN pip install -U sentence_transformers==2.6.0

# Installing Git LFS to download large files
RUN curl https://github.com/git-lfs/git-lfs/releases/download/v2.9.0/git-lfs-linux-amd64-v2.9.0.tar.gz --output ./git-lfs-linux-amd64-v2.9.0.tar.gz
RUN tar -xf git-lfs-linux-amd64-v2.9.0.tar.gz
RUN chmod 755 install.sh
RUN ./install.sh
RUN rm -rf git-lfs-linux-amd64-v2.9.0.tar.gz


# Installing nodejs app dependencies
COPY package*.json ./
RUN npm install

# Copying local files to the container
COPY . .

# Running the app
RUN cd /usr/src/app

# Setting permissions
RUN chmod -R 755 /bin



