#! /bin/bash

function process() {
    for file in `ls $1`;
    do
        file="$1"$file
        if [ -d $file -a $file != '__pycache__' ] ; then
            echo 'start to process dir: '$file
            process $file"/" $2
            echo 'end to process dir: '$file
        elif [ -f $file -a $2 != $file ] ; then
            echo 'start to process file: '$file
            sed -i 's/\r//' $file
            echo 'end to process file: '$file
        else
           echo 'nothing to process: '$file
        fi
    done
}
# dir_name = `pwd`
process $1 $1$0
# process "/home/fresh_etl/fresh_etl/xhw/demo2/" demo.sh
echo 'hello 我的歌'