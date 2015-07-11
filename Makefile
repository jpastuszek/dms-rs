deps:
	wget http://download.nanomsg.org/nanomsg-0.5-beta.tar.gz
	tar -xvzf nanomsg-0.5-beta.tar.gz
	cd nanomsg-0.5-beta && ./configure && make && make install
	git clone https://github.com/kentonv/capnproto.git
	export CC=gcc-4.8
	export CXX=g++-4.8
	cd capnproto/c++
	./setup-autotools.sh
	autoreconf -i
	./configure && make -j5 && make install && cd ../..

clean:
	rm -rf capnproto
	rm -rf nanomsg-0.5-beta
	rm nanomsg-0.5-beta.tar.gz

.PHONY: clean deps