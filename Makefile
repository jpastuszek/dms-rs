deps:
	wget http://download.nanomsg.org/nanomsg-0.5-beta.tar.gz
	tar -xvzf nanomsg-0.5-beta.tar.gz
	cd nanomsg-0.5-beta && ./configure && make && sudo make install
	git clone https://github.com/kentonv/capnproto.git
	cd capnproto/c++
	./setup-autotools.sh
	autoreconf -i
	./configure && make -j5 && sudo make install && cd ../..
	sudo ldconfig

clean:
	rm -rf capnproto
	rm -rf nanomsg-0.5-beta
	rm nanomsg-0.5-beta.tar.gz

.PHONY: clean deps
