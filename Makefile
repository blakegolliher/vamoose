# vamoose packaging + install.
#
#   make build              release build (native toolchain)
#   make build TARGET=x86_64-unknown-linux-gnu.2.34
#                           cross build via cargo-zigbuild (older glibc)
#   make install            install into DESTDIR (default /)
#   make rpm                build an .rpm into dist/
#   make deb                build a .deb into dist/
#
# Packages contain: vamoose / mig-worker / mig-aggr, an example config
# at /etc/vamoose/vamoose.toml.example, a systemd unit, and (when
# LIBNFS_SO points at a built library) a vendored libnfs under
# /usr/lib/vamoose with an ld.so.conf.d drop-in. libnfs is
# LGPL-2.1-or-later and stays dynamically linked; the patched source
# is published at https://github.com/blakegolliher/libnfs
# (branch vamoose-patches) — keep that pointer accurate if the lib is
# rebuilt from elsewhere.

NAME        := vamoose
VERSION     := $(shell cargo metadata --no-deps --format-version 1 2>/dev/null \
                 | grep -o '"version":"[^"]*"' | head -1 | cut -d'"' -f4)
RELEASE     := 1
ARCH        := x86_64

# Cross target (cargo-zigbuild), e.g. x86_64-unknown-linux-gnu.2.34.
TARGET      :=
# Path to a built libnfs.so.16.* to vendor into the package. Empty =
# the package depends on a system-provided libnfs instead.
LIBNFS_SO   :=

BINS        := vamoose mig-worker mig-aggr
CARGO_FLAGS := --release --locked

ifneq ($(TARGET),)
  CARGO      := cargo zigbuild $(CARGO_FLAGS) --target $(TARGET)
  # cargo puts artifacts under the triple without the glibc suffix.
  TRIPLE     := $(firstword $(subst ., ,$(TARGET))).$(word 2,$(subst ., ,$(TARGET)))
  TARGET_DIR := target/$(basename $(TARGET))/release
else
  CARGO      := cargo build $(CARGO_FLAGS)
  TARGET_DIR := target/release
endif

DESTDIR     :=
PREFIX      := /usr
BINDIR      := $(PREFIX)/bin
UNITDIR     := $(PREFIX)/lib/systemd/system
SYSCONFDIR  := /etc/$(NAME)
VENDORLIB   := $(PREFIX)/lib/$(NAME)
DOCDIR      := $(PREFIX)/share/doc/$(NAME)

DIST        := dist
STAGE       := $(DIST)/stage

.PHONY: all build install stage rpm deb clean version

all: build

version:
	@echo $(VERSION)

build:
	$(CARGO) -p vamoose-cli -p migration-worker -p migration-aggr

# DESTDIR-aware install of everything the packages ship.
install:
	install -d $(DESTDIR)$(BINDIR)
	for b in $(BINS); do \
	    install -m 0755 $(TARGET_DIR)/$$b $(DESTDIR)$(BINDIR)/$$b; \
	done
	install -d $(DESTDIR)$(SYSCONFDIR)
	install -m 0644 examples/worker.toml \
	    $(DESTDIR)$(SYSCONFDIR)/vamoose.toml.example
	install -d $(DESTDIR)$(UNITDIR)
	install -m 0644 examples/vamoose-worker.service \
	    $(DESTDIR)$(UNITDIR)/vamoose-worker.service
	install -d $(DESTDIR)$(DOCDIR)
	install -m 0644 README.md $(DESTDIR)$(DOCDIR)/README.md
	install -m 0644 THIRD_PARTY_LICENSES.md \
	    $(DESTDIR)$(DOCDIR)/THIRD_PARTY_LICENSES.md
ifneq ($(LIBNFS_SO),)
	install -d $(DESTDIR)$(VENDORLIB)
	install -m 0755 $(LIBNFS_SO) \
	    $(DESTDIR)$(VENDORLIB)/$(notdir $(LIBNFS_SO))
	install -d $(DESTDIR)/etc/ld.so.conf.d
	echo $(VENDORLIB) > $(DESTDIR)/etc/ld.so.conf.d/$(NAME).conf
	printf 'Vendored libnfs: LGPL-2.1-or-later, dynamically linked.\n\
Source for this exact build: \
https://github.com/blakegolliher/libnfs (branch vamoose-patches)\n' \
	    > $(DESTDIR)$(DOCDIR)/LIBNFS_SOURCE.txt
endif

stage: build
	rm -rf $(STAGE)
	$(MAKE) install DESTDIR=$(STAGE)

rpm: stage
	@command -v rpmbuild >/dev/null || \
	    { echo "rpmbuild not found (install the 'rpm' package)"; exit 1; }
	rm -rf $(DIST)/rpmroot
	mkdir -p $(DIST)/rpmroot/SPECS $(DIST)/rpmroot/BUILDROOT
	printf '%s\n' \
	  'Name: $(NAME)' \
	  'Version: $(VERSION)' \
	  'Release: $(RELEASE)' \
	  'Summary: Distributed NFS-to-NFS migration with S3 coordination' \
	  'License: AGPL-3.0-only' \
	  'URL: https://github.com/blakegolliher/vamoose' \
	  'AutoReqProv: no' \
	  '%description' \
	  'Wire-rate NFSv3 file migration: raw-filehandle mover, S3' \
	  'conditional-PUT shard claiming, self-fencing workers, and a' \
	  'coordinator/TUI control plane.' \
	  '%files' \
	  '$(BINDIR)/*' \
	  '%dir $(SYSCONFDIR)' \
	  '$(SYSCONFDIR)/vamoose.toml.example' \
	  '$(UNITDIR)/vamoose-worker.service' \
	  '$(DOCDIR)/*' \
	  > $(DIST)/rpmroot/SPECS/$(NAME).spec
ifneq ($(LIBNFS_SO),)
	printf '%s\n' \
	  '$(VENDORLIB)/*' \
	  '/etc/ld.so.conf.d/$(NAME).conf' \
	  >> $(DIST)/rpmroot/SPECS/$(NAME).spec
	printf '%s\n' '%post' '/sbin/ldconfig' '%postun' '/sbin/ldconfig' \
	  >> $(DIST)/rpmroot/SPECS/$(NAME).spec
endif
	cp -a $(STAGE) \
	  $(DIST)/rpmroot/BUILDROOT/$(NAME)-$(VERSION)-$(RELEASE).$(ARCH)
	rpmbuild -bb \
	  --define '_topdir $(CURDIR)/$(DIST)/rpmroot' \
	  --define '_rpmdir $(CURDIR)/$(DIST)' \
	  --buildroot $(CURDIR)/$(DIST)/rpmroot/BUILDROOT/$(NAME)-$(VERSION)-$(RELEASE).$(ARCH) \
	  $(DIST)/rpmroot/SPECS/$(NAME).spec
	@ls -l $(DIST)/$(ARCH)/*.rpm

deb: stage
	@command -v dpkg-deb >/dev/null || \
	    { echo "dpkg-deb not found"; exit 1; }
	rm -rf $(DIST)/debroot
	cp -a $(STAGE) $(DIST)/debroot
	mkdir -p $(DIST)/debroot/DEBIAN
	printf '%s\n' \
	  'Package: $(NAME)' \
	  'Version: $(VERSION)-$(RELEASE)' \
	  'Architecture: amd64' \
	  'Maintainer: Blake Golliher <blakegolliher@gmail.com>' \
	  'Section: admin' \
	  'Priority: optional' \
	  'Description: Distributed NFS-to-NFS migration with S3 coordination' \
	  ' Wire-rate NFSv3 file migration: raw-filehandle mover, S3' \
	  ' conditional-PUT shard claiming, self-fencing workers, and a' \
	  ' coordinator/TUI control plane.' \
	  > $(DIST)/debroot/DEBIAN/control
ifneq ($(LIBNFS_SO),)
	printf '#!/bin/sh\nldconfig\n' > $(DIST)/debroot/DEBIAN/postinst
	printf '#!/bin/sh\nldconfig\n' > $(DIST)/debroot/DEBIAN/postrm
	chmod 0755 $(DIST)/debroot/DEBIAN/postinst $(DIST)/debroot/DEBIAN/postrm
endif
	dpkg-deb --build --root-owner-group $(DIST)/debroot \
	  $(DIST)/$(NAME)_$(VERSION)-$(RELEASE)_amd64.deb
	@ls -l $(DIST)/*.deb

clean:
	rm -rf $(DIST)
