default:
	@echo "targets: appimage (Linux only), clean, debug, package, release"

appimage-prep:
	# There is something about the Docker CentOS 6 image such that the linking
	# step fails for libbsd even though cmake is able to find the library and
	# set up the -l flags correctly.  If we explicitly set the -L flag to the
	# directory we know that it has been installed in, then linking works.
	cmake -H. -Bbuild/appimage -DCMAKE_INSTALL_PREFIX=/usr
	cd build/appimage && make
	cd build/appimage && sed -i -e 's#/usr#././#g' pg_systat
	cd build/appimage && make install DESTDIR=AppDir

appimage: appimage-prep
	cd build/appimage && make appimage

appimage-docker: appimage-prep
	cd build/appimage && make appimage-docker

clean:
	-rm -rf build

debug:
	cmake -H. -Bbuild/debug -DCMAKE_BUILD_TYPE=Debug
	cd build/debug && make

package:
	git checkout-index --prefix=build/source/ -a
	cmake -Hbuild/source -Bbuild/source
	cd build/source && make package_source

release:
	cmake -H. -Bbuild/release
	cd build/release && make
