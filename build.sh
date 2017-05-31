curl -L https://github.com/edenhill/librdkafka/archive/v0.9.2-RC1.tar.gz | tar xzf -
cd librdkafka-0.9.2-RC1/
./configure --prefix=/usr
make -j
sudo make install

pip3 install -r ../requirements.txt
