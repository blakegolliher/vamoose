# On a host that can mount the source NFS export (kernel mount is fine 
# for *creating* the test tree — the worker uses libnfs to *read* it later)
sudo mount -t nfs <src-server>:/export /mnt/src-test
mkdir -p /mnt/vamoose-source/src-test/m2-verify
cd /mnt/vamoose-source/src-test/m2-verify

# Variety of sizes
: > empty.bin                                    # 0 bytes
echo "x" > tiny.txt                              # 1 byte-ish
dd if=/dev/urandom of=small.bin bs=1K count=1    # 1 KiB
dd if=/dev/urandom of=medium.bin bs=1M count=1   # 1 MiB
dd if=/dev/urandom of=large.bin bs=1M count=100  # 100 MiB

# Subdirs
mkdir -p sub/deep/nested
dd if=/dev/urandom of=sub/deep/nested/file.bin bs=1K count=4

# Various modes
chmod 644 small.bin
chmod 600 medium.bin
chmod 755 large.bin

# Various owners (need root on the host)
sudo chown 1000:1000 small.bin
sudo chown 1234:5678 medium.bin

# Symlinks — both relative and absolute
ln -s small.bin link-rel.bin
ln -s /etc/hostname link-abs.bin

# Hardlink group
echo "shared content" > hl-original.txt
ln hl-original.txt hl-link-1.txt
ln hl-original.txt hl-link-2.txt

# THE ONE I KEEP HARPING ON: non-UTF-8 path
# Create a file whose name contains a 0xff byte
python3 -c 'open(b"weird-\xff-name.bin", "wb").write(b"contents")'

# And one with embedded spaces and a newline (yes, real)
touch "file with spaces.txt"
touch "$(printf 'file\nwith\nnewline')"
