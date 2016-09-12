#!/bin/bash
for d in */ ; do
    cd "$d"
    pwd
    for f in *.tar.gz; do
        if [ -f "$f" ];
        then
            echo "$f"
            tar -zxvf "$f"
    	    rm "$f"
        fi
    done
    for f in */ ; do
        echo "$f"
	cd $f
	if [ -f gureKddcup.list.tar.gz ];
	then 
	    tar -zxvf gureKddcup.list.tar.gz
	    rm gureKddcup.list.tar.gz
	fi
	cd ..
    done
    cd ..
done
