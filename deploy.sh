#! /bin/sh

src=~/src/CIDgravity-gw/kuri
target=~/bin/
symlink=kuri

usage() {
        cat <<EOF >&2
Usage:
    $0 <reason>

Deploy kuri as binary with timestamp and <reason> in name
EOF
        exit 1
}
die() {
        echo "$@" >&2
        exit 1
}
case "$#-$1" in
        1--*) usage ;;
        */*) usage ;;
        *" "*) usage ;;
        1-*) reason="$1" ;;
        *) usage ;;
esac

mt=$(stat --format=%Y "$src") || die "Failed to stat $src"
dt=$(date -d @"$mt" +"%Y%m%d.%H%M%S") || die "Failed to format date"
name="kuri-$dt-$reason"
mv -i "$src" "$target/$name" || die "Failed to move binary"
ln -sf "$name" "$target/$symlink"
